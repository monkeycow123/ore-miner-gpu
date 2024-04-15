use std::{
    sync::{atomic::AtomicUsize, Arc}, time::{Duration, Instant}
};

use clap::Parser;
use itertools::Itertools;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
};
use tokio::sync::{
    mpsc::{channel, Sender},
    RwLock,
};
use tracing::{debug, error, info, warn};

use crate::{
    constant,
    format_duration,
    format_reward,
    jito::{subscribe_jito_tips, JitoTips},
    utils,
    wait_return,
    Miner,
};

#[derive(Debug, Clone, Parser)]
pub struct BundleMineGpuArgs {
    #[arg(long, help = "The folder that contains all the keys used to claim $ORE")]
    pub key_folder: String,

    #[arg(
        long,
        default_value = "0",
        help = "The maximum tip to pay for jito. Set to 0 to disable adaptive tip"
    )]
    pub max_adaptive_tip: u64,

    #[arg(long, default_value = "2", help = "The maximum number of buses to use for mining")]
    pub max_buses: usize,
}

impl Miner {
    pub async fn bundle_mine_gpu(&self, args: &BundleMineGpuArgs) {
        if args.max_buses == 0 {
            panic!("max buses must be greater than 0");
        }

        let client = Miner::get_client_confirmed(&self.rpc);

        let all_signers = Self::read_keys(&args.key_folder)
            .into_iter()
            .map(Box::new)
            .collect::<Vec<_>>();

        if all_signers.len() % Accounts::size() != 0 {
            panic!("number of keys must be a multiple of {}", Accounts::size());
        }

        info!("{} keys loaded", all_signers.len());

        let idle_accounts_counter = Arc::new(AtomicUsize::new(all_signers.len()));

        // Setup channels
        let (ch_accounts, mut ch_accounts_receiver) = channel::<Accounts>(all_signers.len() / Accounts::size());

        let batches = all_signers
            .into_iter()
            .chunks(Accounts::size())
            .into_iter()
            .enumerate()
            .map(|(i, signers)| {
                let signers = signers.collect::<Vec<_>>();

                Accounts {
                    id: i,
                    pubkey: signers.iter().map(|k| k.pubkey()).collect(),
                    proof_pda: signers
                        .iter()
                        .map(|k| utils::get_proof_pda_no_cache(k.pubkey()))
                        .collect(),
                    signers,
                    release_stuff: (ch_accounts.clone(), idle_accounts_counter.clone()),
                }
            })
            .collect::<Vec<_>>();

        for signers in batches {
            ch_accounts.send(signers).await.unwrap();
        }

        info!("splitted signers into batches");

        // Subscribe tip stream
        let tips = Arc::new(RwLock::new(JitoTips::default()));
        subscribe_jito_tips(tips.clone()).await;
        info!("subscribed to jito tip stream");

        loop {
            let mut batch = Vec::new();

            while let Ok(accounts) = ch_accounts_receiver.try_recv() {
                batch.push(accounts);
                if batch.len() >= 4 {
                    break;
                }
            }

            if batch.is_empty() {
                debug!("no more batches, waiting for more signers");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let idle_accounts = idle_accounts_counter
                .fetch_sub(batch.len() * Accounts::size(), std::sync::atomic::Ordering::Relaxed) -
                batch.len() * Accounts::size();

            loop {
                let result = self
                    .mine_with_accounts(client.clone(), batch, idle_accounts)
                    .await;

                batch = match result {
                    Some(batch_to_retry) => batch_to_retry,
                    None => break,
                }
            }
        }
    }

    async fn mine_with_accounts(
        &self,
        client: Arc<RpcClient>,
        batch: Vec<Accounts>,
        idle_accounts: usize,
    ) -> Option<Vec<Accounts>> {
        let (mut treasury, mut clock, mut buses) = match Self::get_system_accounts(&client).await {
            Ok(accounts) => accounts,
            Err(err) => {
                error!("fail to fetch system accounts: {err:#}");
                wait_return!(500, Some(batch));
            }
        };

        let all_pubkey = batch
            .iter()
            .flat_map(|accounts| accounts.pubkey.clone())
            .collect::<Vec<_>>();

        let proof_pda = batch
            .iter()
            .flat_map(|accounts| accounts.proof_pda.clone())
            .collect::<Vec<_>>();

        let signer_balances = match Self::get_balances(&client, &all_pubkey).await {
            Ok(b) => b,
            Err(err) => {
                error!("fail to get signers balances: {err:#}");
                wait_return!(500, Some(batch));
            }
        };

        let proofs = match Self::get_proof_accounts(&client, &proof_pda).await {
            Ok(proofs) => proofs,
            Err(err) => {
                error!("fail to fetch proof accounts: {err:#}");
                wait_return!(500, Some(batch));
            }
        };

        let hash_and_pubkey = all_pubkey
            .iter()
            .zip(proofs.iter())
            .map(|(signer, proof)| (solana_sdk::keccak::Hash::new_from_array(proof.hash.0), *signer))
            .collect::<Vec<_>>();
        let (mining_duration, mining_results) = self
            .mine_hashes_gpu(&treasury.difficulty.into(), &hash_and_pubkey)
            .await;

        // if mining_duration > time_to_next_epoch {
        //     warn!("mining took too long, waiting for next epoch");
        //     wait_return!(time_to_next_epoch.as_millis() as u64, Some(batch));
        // } else {
        //     info!(
        //         accounts = Accounts::size() * batch.len(),
        //         accounts.idle = idle_accounts,
        //         mining = format_duration!(mining_duration),
        //         "mining done"
        //     );
        // }

        info!(
            accounts = Accounts::size() * batch.len(),
            accounts.idle = idle_accounts,
            mining = format_duration!(mining_duration),
            "mining done"
        );

        let reset_threshold = treasury.last_reset_at.saturating_add(ore::EPOCH_DURATION);
        debug!("buses: {}, clock: {}", buses.len(), clock.epoch);
        (treasury, clock, buses) = match Self::get_system_accounts(&client).await {
            Ok(accounts) => accounts,
            Err(err) => {
                error!("fail to fetch system accounts: {err:#}");
                wait_return!(500, Some(batch));
            }
        };
        
        let mut time_to_next_epoch = Self::get_time_to_next_epoch(&treasury, &clock, reset_threshold).checked_add(Duration::from_secs(10)).expect("overflow");

        let mut cummulative_reward = 0;
        let target_reward = treasury.reward_rate.saturating_mul(all_pubkey.len() as u64 )*2;
        buses.sort_by(|a, b| b.rewards.cmp(&a.rewards));

        while cummulative_reward < target_reward {
            
            for bus in buses.iter(){
                cummulative_reward += bus.rewards;
            }

            if cummulative_reward < target_reward {
                warn!("no bus available for mining, waiting for next epoch",);
                cummulative_reward = 0;
                // wait_return!(time_to_next_epoch.as_millis() as u64, Some(batch));
                tokio::time::sleep( time_to_next_epoch ).await;
                (treasury, clock, buses) = match Self::get_system_accounts(&client).await {
                    Ok(accounts) => accounts,
                    Err(err) => {
                        error!("fail to fetch system accounts: {err:#}");
                        wait_return!(500, Some(batch));
                    }
                };
                
                time_to_next_epoch = Self::get_time_to_next_epoch(&treasury, &clock, reset_threshold).checked_add(Duration::from_secs(10)).expect("overflow");
        
            }
        }

        
        let payer = utils::pick_richest_account(&signer_balances, &all_pubkey);

        match self.send_and_confirm(
            self.build_ixs(&batch, mining_results, Vec::from(buses), treasury.reward_rate),
            false,
            false,
            payer,
            &batch).await{
                Ok(sig) => {
                    println!("Success: {}", sig);
                }
                Err(_err) => {
                    println!("Error: Failed to send transaction");
                }
            }

        None
    }
}


pub struct Accounts {
    pub id: usize,
    #[allow(clippy::vec_box)]
    pub signers: Vec<Box<Keypair>>,
    pub pubkey: Vec<Pubkey>,
    pub proof_pda: Vec<Pubkey>,
    release_stuff: (Sender<Accounts>, Arc<AtomicUsize>),
}

impl Accounts {
    pub async fn release(self) {
        self.release_stuff
            .1
            .fetch_add(Self::size(), std::sync::atomic::Ordering::Relaxed);

        self.release_stuff
            .0
            .clone()
            .send(self)
            .await
            .expect("failed to release accounts");
    }

    pub const fn size() -> usize {
        2
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn watch_signatures(
        self,
        client: Arc<RpcClient>,
        signatures: Vec<Signature>,
        tip: u64,
        tips: Arc<RwLock<JitoTips>>,
        send_at_slot: Slot,
        sent_at_time: Instant,
        rewards: u64,
    ) {
        let mut latest_slot = send_at_slot;
        let mut landed_tx = vec![];

        while landed_tx.is_empty() && latest_slot < send_at_slot + constant::SLOT_EXPIRATION {
            tokio::time::sleep(Duration::from_secs(2)).await;
            debug!(
                acc.id = self.id,
                slot.current = latest_slot,
                slot.send = send_at_slot,
                ?signatures,
                "checking bundle status"
            );

            let (statuses, slot) = match Miner::get_signature_statuses(&client, &signatures).await {
                Ok(value) => value,
                Err(err) => {
                    error!(
                        acc.id = self.id,
                        slot.current = latest_slot,
                        slot.send = send_at_slot,
                        "fail to get bundle status: {err:#}"
                    );

                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            latest_slot = slot;
            landed_tx = utils::find_landed_txs(&signatures, statuses);
        }

        if !landed_tx.is_empty() {
            let cost = Accounts::size() as u64 * constant::FEE_PER_SIGNER + tip;

            info!(
                acc.id = self.id,
                confirm = format_duration!(sent_at_time.elapsed()),
                rewards = format_reward!(rewards),
                cost = format_reward!(cost),
                tip,
                tx.first = ?landed_tx.first().unwrap(),
                "bundle mined",
            );
        } else {
            let tips = *tips.read().await;

            warn!(
                acc.id = self.id,
                confirm = format_duration!(sent_at_time.elapsed()),
                tip,
                tips.p25 = tips.p25(),
                tips.p50 = tips.p50(),
                "bundle dropped"
            );
        }

        self.release().await;
    }
}