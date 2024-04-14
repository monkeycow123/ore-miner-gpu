use std::{
    collections::VecDeque, io::{stdout, Write}, time::Duration
};

use itertools::Itertools;
use ore::{state::{Bus, Hash as OreHash}, BUS_ADDRESSES};
use solana_client::{
    client_error::{ClientError, ClientErrorKind, Result as ClientResult},
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_program::instruction::Instruction;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel}, compute_budget::ComputeBudgetInstruction, keccak::Hash, pubkey::Pubkey, signature::{Signature, Signer}, transaction::Transaction
};
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use crate::bundle_mine_gpu::Accounts;

use crate::Miner;

const RPC_RETRIES: usize = 0;
const SIMULATION_RETRIES: usize = 4;
const GATEWAY_RETRIES: usize = 60;

const CONFIRM_DELAY: u64 = 7000;
const GATEWAY_DELAY: u64 = 1200;
const CU_LIMIT_MINE: u32 = 2530;

const CONFIRM_ROUND_DELAY: u64 = 15_000;
const CONFIRM_ROUND_RETRIES: usize = 4;

impl Miner {

    pub fn priority_fee(&self) -> u64 {
        self.priority_fee.expect("Priority fee not set")
    }

    pub async fn send_and_confirm(
        &self,
        ixs: Vec<Instruction>,
        dynamic_cus: bool,
        skip_confirm: bool,
        signer_key: Pubkey,
        accounts: &Vec<Accounts>,
    ) -> ClientResult<Signature> {
        let mut stdout = stdout();
        let client =
            RpcClient::new_with_commitment(self.rpc.clone(), CommitmentConfig::finalized());

        // Return error if balance is zero
        // let balance = client
        //     .get_balance_with_commitment(&signer.pubkey(), CommitmentConfig::confirmed())
        //     .await
        //     .unwrap();
        // if balance.value <= 0 {
        //     return Err(ClientError {
        //         request: None,
        //         kind: ClientErrorKind::Custom("Insufficient SOL balance".into()),
        //     });
        // }

        let signers = accounts.iter().map(|x| &x.signers).flatten().map(|x| x).collect::<Vec<_>>();

        // Build tx
        let (mut hash, mut slot) = client
            .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
            .await
            .unwrap();
        let mut send_cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Confirmed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(RPC_RETRIES),
            min_context_slot: Some(slot),
        };
        let mut tx = Transaction::new_with_payer(&ixs, Some(&signer_key));

        // Simulate if necessary
        if dynamic_cus {
            let mut sim_attempts = 0;
            'simulate: loop {
                let sim_res = client
                    .simulate_transaction_with_config(
                        &tx,
                        RpcSimulateTransactionConfig {
                            sig_verify: false,
                            replace_recent_blockhash: true,
                            commitment: Some(CommitmentConfig::finalized()),
                            encoding: Some(UiTransactionEncoding::Base64),
                            accounts: None,
                            min_context_slot: None,
                            inner_instructions: false,
                        },
                    )
                    .await;
                match sim_res {
                    Ok(sim_res) => {
                        if let Some(err) = sim_res.value.err {
                            println!("Simulaton error: {:?}", err);
                            sim_attempts += 1;
                            if sim_attempts.gt(&SIMULATION_RETRIES) {
                                return Err(ClientError {
                                    request: None,
                                    kind: ClientErrorKind::Custom("Simulation failed".into()),
                                });
                            }
                        } else if let Some(units_consumed) = sim_res.value.units_consumed {
                            println!("Dynamic CUs: {:?}", units_consumed);
                            let cu_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(
                                units_consumed as u32 + 300,
                            );
                            let cu_price_ix =
                                ComputeBudgetInstruction::set_compute_unit_price(self.priority_fee());
                            let mut final_ixs = vec![];
                            final_ixs.extend_from_slice(&[cu_budget_ix, cu_price_ix]);
                            final_ixs.extend_from_slice(&ixs);
                            tx = Transaction::new_with_payer(&final_ixs, Some(&signer_key));
                            break 'simulate;
                        }
                    }
                    Err(err) => {
                        println!("Simulaton error: {:?}", err);
                        sim_attempts += 1;
                        if sim_attempts.gt(&SIMULATION_RETRIES) {
                            return Err(ClientError {
                                request: None,
                                kind: ClientErrorKind::Custom("Simulation failed".into()),
                            });
                        }
                    }
                }
            }
        }

        // Submit tx
        tx.sign(&signers, hash);
        let mut sigs = VecDeque::new();
        let mut attempts = 0;
        let mut currently_confirmed = false;
        let mut gateway_attempts = 0;
        loop {
            println!("Attempt: {:?}", attempts);
            if !currently_confirmed {
                match client.send_transaction_with_config(&tx, send_cfg).await {
                    Ok(sig) => {
                        sigs.push_back(sig);
                        if sigs.len() > 100{
                            sigs.pop_front();
                            sigs.make_contiguous();
                        }
                        println!("{:?}", sig);

                        // Confirm tx
                        if skip_confirm {
                            return Ok(sig);
                        }
                    }

                    // Handle submit errors
                    Err(err) => {
                        println!("Error {:?}", err);
                    }
                }
            }
            
            let mut any_confirmed = false;

            // Confirm tx while handling forks
            if (attempts % 1 == 0 && attempts > 0) || attempts == GATEWAY_RETRIES {
                std::thread::sleep(Duration::from_millis(CONFIRM_DELAY));
                match client.get_signature_statuses(sigs.as_slices().0).await {
                    Ok(signature_statuses) => {
                        println!("Confirms: {:?}", signature_statuses.value);
                        let mut cnt = 0;
                        for signature_status in signature_statuses.value {
                            if let Some(signature_status) = signature_status.as_ref() {
                                if signature_status.confirmation_status.is_some() {
                                    let current_commitment = signature_status
                                        .confirmation_status
                                        .as_ref()
                                        .unwrap();
                                    match current_commitment {
                                        TransactionConfirmationStatus::Processed => {
                                            if !any_confirmed {
                                                currently_confirmed = false;
                                            }
                                        }
                                        TransactionConfirmationStatus::Confirmed => {
                                            std::thread::sleep(Duration::from_millis(CONFIRM_DELAY));
                                            currently_confirmed = true;
                                            any_confirmed = true;
                                        }
                                        TransactionConfirmationStatus::Finalized => {
                                            println!("Transaction landed!");
                                            return Ok(sigs[cnt]);
                                        }
                                    }
                                } else {
                                    println!("No status");
                                }
                            }
                            cnt += 1;
                        }
                    }

                    // Handle confirmation errors
                    Err(err) => {
                        println!("Error: {:?}", err);
                    }
                }
                
                println!("Transactions did not land");
            }

            stdout.flush().ok();

            // Retry
            std::thread::sleep(Duration::from_millis(GATEWAY_DELAY));
            (hash, slot) = client
                .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
                .await
                .unwrap();
            send_cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Finalized),
                encoding: Some(UiTransactionEncoding::Base64),
                max_retries: Some(RPC_RETRIES),
                min_context_slot: Some(slot),
            };
            tx.sign(&signers, hash);
            attempts += 1;
            if attempts > GATEWAY_RETRIES {
                attempts = 0;
                if gateway_attempts > CONFIRM_ROUND_RETRIES {
                    return Err(ClientError {
                        request: None,
                        kind: ClientErrorKind::Custom("Max retries".into()),
                    });
                }
                gateway_attempts += 1;
                std::thread::sleep(Duration::from_millis(CONFIRM_ROUND_DELAY));
            }
        }
    }

    
    pub fn build_ixs(&self, accounts: &Vec<Accounts>, hash_nonce_pair: Vec<(Hash, u64)>, buses: Vec<Bus>, reward_rate: u64) -> Vec<Instruction>{
        let mut ixs = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_MINE * hash_nonce_pair.len() as u32),
            ComputeBudgetInstruction::set_compute_unit_price(self.priority_fee())
        ];

        let hash_chunks = hash_nonce_pair.iter()
            .chunks(Accounts::size()).into_iter()
            .map(|x| x.collect::<Vec<_>>()).into_iter()
            .zip(accounts.iter()).collect::<Vec<_>>();

        let mut used_bus = 0;
        let mut curr_bus = 0;

        for(hash_chunk, acts) in hash_chunks{
           let hash_nonce_signer = hash_chunk.iter().zip(acts.signers.iter()).map(|(hash_nonce, signer)| {
                let (hash, nonce) = hash_nonce;
                let signer = signer;
                (hash, nonce, signer)
            }).collect::<Vec<_>>();

            for (hash, nonce, signer) in hash_nonce_signer{
                ixs.push(ore::instruction::mine(
                    signer.pubkey(),
                    BUS_ADDRESSES[buses[curr_bus].id as usize],
                    OreHash::from(Hash::new_from_array(hash.to_bytes())), 
                    *nonce,
                ));

                used_bus += reward_rate;
                if used_bus as f64 > 0.5*(buses[curr_bus].rewards as f64) {
                    used_bus = 0;
                    curr_bus += 1;
                }
            }
        }
        
        ixs
    }

}
