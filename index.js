import axios from 'axios';
import knex from 'knex';
import cron from 'node-cron';
import got from 'got';
import dotenv from 'dotenv';

dotenv.config();

const LAG_N_BLOCKS = 2
const FA2_CONTRACT = 'KT1J6NY5AU61GzUX51n59wwiZcGJ9DrNTwbK';
const MARKET_CONTRACT = 'KT1VoZeuBMJF6vxtLqEFMoc4no5VDG789D7z';
const REGISTER_CONTRACT = 'KT1QSERFwHZcQfUf6ZruTSLMBfzitRs6ixmP';
let current_level;
let last_level;
let start = false;
const config = {
  client: 'pg',
  connection: {
    connectionString: process.env.HEROKU_POSTGRESQL_AQUA_URL,
    ssl: { rejectUnauthorized: false },
  },
  pool: { min: 2, max: 7 },
};
const db = knex(config);

async function set_last_indexed_level() {
  const results = await db.select('*').from('public.levels').where('id', '=', 1);
  current_level = results[0];
  start = true;
}

async function mint() {
  let m_address; let m_amount; let m_token_id; let meta_uri; let meta_name; let meta_des;
  try {
    const mint_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.mint}&target.in=${FA2_CONTRACT}&entrypoint.eq=mint&select=parameter&status=applied`);
    // console.log(mint_index.data)
    if (mint_index.data.length !== 0) {
      for (let i = 0; i < mint_index.data.length; i++) {
        m_address = mint_index.data[i].value.address;
        m_amount = mint_index.data[i].value.amount;
        m_token_id = mint_index.data[i].value.token_id;

        const ipfs = `https://typed.infura-ipfs.io/ipfs/${decodeURIComponent(mint_index.data[i].value.token_info[''].replace(/(..)/g, '%$1')).substr(7)}`;
        try {
          meta_uri = decodeURIComponent(mint_index.data[i].value.token_info[''].replace(/(..)/g, '%$1'));
          const metadata = await got(ipfs, { timeout: { request: 3000 } }).json();
          meta_name = metadata.name;
          meta_des = metadata.description;
          console.log('mintlenen--------------');
          console.log(m_address, m_amount, m_token_id);
          await db('public.tokens').insert({
            token_id: parseInt(m_token_id),
            metadata_uri: meta_uri,
            editions: parseInt(m_amount),
            burned: 0,
            minter_address: m_address,
            name: meta_name,
            description: meta_des,
            metadata_index: true,
            level: current_level.mint,
          }).onConflict('token_id').ignore();
          try {
            await db('public.holders').insert(
              {
                holder_address: m_address,
                token_id: m_token_id,
                amount: parseInt(m_amount),
                level: current_level.mint,
              },
            );
          } catch (e) {
            console.error(e);
          }
        } catch (e) {
          console.log('meta cekilemedi');
          console.log(m_address, m_amount, m_token_id);
          const results = await db('public.tokens').insert({
            token_id: parseInt(m_token_id),
            metadata_uri: meta_uri,
            editions: parseInt(m_amount),
            burned: 0,
            minter_address: m_address,
            name: meta_name,
            description: meta_des,
            metadata_index: false,
            level: current_level.mint,
          }).onConflict('token_id').ignore();
          try {
            const putholders = await db('public.holders').insert(
              {
                holder_address: m_address,
                token_id: m_token_id,
                amount: parseInt(m_amount),
                level: current_level.mint,
              },
            );
          } catch (e) {
            console.error(e);
          }
        }
      }
      if (current_level.mint < last_level) {
        current_level.mint = parseInt(current_level.mint) + 1;
        mint(current_level.mint);
      } else {
        yaz = await db('public.levels').where('id', '=', 1).update({ mint: current_level.mint });
        console.log(`son_mint_level:${current_level.mint}`);
        // transfer();
        swap();
      }
    } else {
    // console.log("bos")
      if (current_level.mint < last_level) {
        current_level.mint = parseInt(current_level.mint) + 1;
        mint(current_level.mint);
      } else {
        yaz = await db('public.levels').where('id', '=', 1).update({ mint: current_level.mint });
        console.log(`son_mint_level:${current_level.mint}`);
        /// transfer();
        swap();
      }
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.mint, state: 'mint' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function transfer() {
  let yaz;
  try {
    const transfer_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.holder}&target.in=${FA2_CONTRACT}&entrypoint.eq=transfer&select=diffs&status=applied`);
    if (transfer_index.data.length !== 0) {
      if (!transfer_index.data.includes(null)) {
        for (let i = 0; i < transfer_index.data.length; i++) {
          for (let xx = 0; xx < transfer_index.data[i].length; xx++) {
            const id = transfer_index.data[i][xx].content.key.nat;
            const adres = transfer_index.data[i][xx].content.key.address;
            const val = transfer_index.data[i][xx].content.value;
            const results = await db.select('*').from('public.holders').where('holder_address', '=', adres).andWhere('token_id', '=', id);
            if (results.length == 1) {
              console.log(`transfer ${id}`);
              const xd = await db.select('*').from('public.holders').where('holder_address', '=', adres).andWhere('token_id', '=', id)
                .update(
                  {
                    holder_address: adres,
                    token_id: id,
                    amount: val,
                    level: current_level.holder,
                  },
                );
            } else {
              console.log(`indexlenmemis ${id}`);
              try {
                const eresult = await db('public.holders').insert(
                  {
                    holder_address: adres,
                    token_id: id,
                    amount: val,
                    level: current_level.holder,
                  },
                );
              } catch (e) {
                console.error(e);
              }
            }
          }
        }
        if (current_level.holder < last_level) {
          current_level.holder = parseInt(current_level.holder) + 1;
          transfer(current_level.holder);
        } else {
          yaz = await db('public.levels').where('id', '=', 1).update({ holder: current_level.holder });
          console.log(`son_holder_level:${current_level.holder}`);
        }
      } else if (current_level.holder < last_level) {
        current_level.holder = parseInt(current_level.holder) + 1;
        transfer(current_level.holder);
      } else {
        yaz = await db('public.levels').where('id', '=', 1).update({ holder: current_level.holder });
      }
    } else if (current_level.holder < last_level) {
      current_level.holder = parseInt(current_level.holder) + 1;
      transfer(current_level.holder);
    } else {
      yaz = await db('public.levels').where('id', '=', 1).update({ holder: current_level.holder });
      console.log(`son_holder_level:${current_level.holder}`);
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.holder, state: 'holder' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function swap() {
  try {
    const swap_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.swap}&target.in=${MARKET_CONTRACT}&entrypoint.eq=swap&select=diffs&status=applied`);
    if (swap_index.data.length !== 0) {
      for (let i = 0; i < swap_index.data.length; i++) {
        try {
          const pubswap = await db('public.swaps').insert(
            {
              token_id: swap_index.data[i][0].content.value.objkt_id,
              seller_address: swap_index.data[i][0].content.value.issuer,
              minter_address: swap_index.data[i][0].content.value.creator,
              price: parseInt(swap_index.data[i][0].content.value.xtz_per_objkt),
              amount: parseInt(swap_index.data[i][0].content.value.objkt_amount),
              amount_left: parseInt(swap_index.data[i][0].content.value.objkt_amount),
              swap_id: parseInt(swap_index.data[i][0].content.key),
              level: current_level.swap,
            },
          );
          console.log(`swap: ${swap_index.data[i][0].content.value.objkt_id}`);
        } catch (e) {
          console.log('token indexlenmemis');
          console.error(e);
        }
      }
      if (current_level.swap < last_level) {
        current_level.swap = parseInt(current_level.swap) + 1;
        swap(current_level.swap);
      } else {
        await db('public.levels').where('id', '=', 1).update({ swap: current_level.swap });
        console.log(`son_swap_level:${current_level.swap}`);
        cancel_swap();
        burn();
        // collect();
      }
    } else if (current_level.swap < last_level) {
      current_level.swap = parseInt(current_level.swap) + 1;
      swap(current_level.swap);
    } else {
      await db('public.levels').where('id', '=', 1).update({ swap: current_level.swap });
      console.log(`son_swap_level:${current_level.swap}`);
      cancel_swap();
      burn();
      // collect();
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.swap, state: 'swap' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function cancel_swap() {
  try {
    const cancel_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.cancel_swap}&target.in=${MARKET_CONTRACT}&entrypoint.eq=cancel_swap&select=parameter`);
    if (cancel_index.data.length !== 0) {
      for (let i = 0; i < cancel_index.data.length; i++) {
        try {
          await db('public.swaps').where('swap_id', '=', cancel_index.data[i].value).del();
          console.log(`swap_cancel: ${cancel_index.data[i].value}`);
        } catch (e) {
          console.log(`cancelswap_hatasi:${current_level.cancel_swap}`);
          console.error(e);
        }
      }
      if (current_level.cancel_swap < last_level) {
        current_level.cancel_swap = parseInt(current_level.cancel_swap) + 1;
        cancel_swap(current_level.cancel_swap);
      } else {
        await db('public.levels').where('id', '=', 1).update({ cancel_swap: current_level.cancel_swap });
        console.log(`son_cancel_swap_level:${current_level.cancel_swap}`);
        collect();
      }
    } else if (current_level.cancel_swap < last_level) {
      current_level.cancel_swap = parseInt(current_level.cancel_swap) + 1;
      cancel_swap(current_level.cancel_swap);
    } else {
      await db('public.levels').where('id', '=', 1).update({ cancel_swap: current_level.cancel_swap });
      console.log(`son_cancel_swap_level:${current_level.cancel_swap}`);
      collect();
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.cancel_swap, state: 'cancel_swap' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function collect() {
  let yaz;
  try {
    const collect_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.collect}&target.in=${MARKET_CONTRACT}&entrypoint.eq=collect&select=diffs`);
    if (collect_index.data.length !== 0) {
      for (let i = 0; i < collect_index.data.length; i++) {
        try {
          if (collect_index.data[i][0].action == 'remove_key') {
            try {
              yaz = await db('public.swaps').where('swap_id', '=', collect_index.data[i][0].content.key).del();
              console.log(`hepsi alindi: ${collect_index.data[i][0].content.key}`);
            } catch (e) {
              console.log(`collect_hatasi:${current_level.collect}`);
              console.error(e);
            }
          } else {
            try {
              yaz = await db('public.swaps').where('swap_id', '=', collect_index.data[i][0].content.key).update({ amount_left: collect_index.data[i][0].content.value.objkt_amount, level: current_level.collect });
              console.log(`collect:${collect_index.data[i][0].content.key}`);
            } catch (e) {
              console.log(`collect_hatasi:${current_level.collect}`);
              console.error(e);
            }
          }
        } catch (e) {
          console.log(`collect_hatasi:${current_level.collect}`);
          console.error(e);
        }
      }
      if (current_level.collect < last_level) {
        current_level.collect = parseInt(current_level.collect) + 1;
        collect(current_level.collect);
      } else {
        yaz = await db('public.levels').where('id', '=', 1).update({ collect: current_level.collect });
        console.log(`son_collect_level:${current_level.collect}`);
      }
    } else if (current_level.collect < last_level) {
      current_level.collect = parseInt(current_level.collect) + 1;
      collect(current_level.collect);
    } else {
      yaz = await db('public.levels').where('id', '=', 1).update({ collect: current_level.collect });
      console.log(`son_collect_level:${current_level.collect}`);
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.collect, state: 'collect' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function metadata_fixer() {
  let t_id; let meta_name; let meta_des;
  try {
    const meta_fixed = await db('public.tokens').where('metadata_index', '=', false).andWhere('description', '!=', 'null').update({ metadata_index: true });
    const meta_reindex = await db('public.tokens').where('metadata_index', '=', false);
    for (let i = 0; i < meta_reindex.length; i++) {
      t_id = meta_reindex[i].token_id;
      try {
        const ipfs = `https://typed.infura-ipfs.io/ipfs/${meta_reindex[i].metadata_uri.substr(7)}`;
        const metadata = await got(ipfs, { timeout: { request: 3000 } }).json();
        meta_name = metadata.name;
        meta_des = metadata.description;
        const results = await db('public.tokens').where('token_id', '=', t_id).update({
          name: meta_name,
          description: meta_des,
          metadata_index: true,
        });
      } catch (e) {
        console.log('ipfs agi hatasi');
        console.error(e);
      }
    }
  } catch (e) {
    console.error(e);
  }
}

async function register() {
  let yaz;
  try {
    const register_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.register}&target.in=${REGISTER_CONTRACT}&entrypoint.eq=register&status=applied`);
    if (register_index.data.length !== 0) {
      for (let i = 0; i < register_index.data.length; i++) {
        try {
          const wallet = register_index.data[i].sender.address;
          const name = decodeURIComponent(register_index.data[i].parameter.value.replace(/(..)/g, '%$1'));
          const results = await db('public.userlist').insert({ wallet_address: wallet, user_name: name, level: current_level.register }).onConflict('wallet_address').merge();
          console.log(`register oldu: ${name}`);
        } catch (e) {
          console.log(`kayit hatasi ${current_level.register}`);
          console.error(e);
        }
      }
      if (current_level.register < last_level) {
        current_level.register = parseInt(current_level.register) + 1;
        register(current_level.register);
      } else {
        yaz = await db('public.levels').where('id', '=', 1).update({ register: current_level.register, level: current_level.register });
        console.log(`son_register_level:${current_level.register}`);
        // mint();
      }
    } else if (current_level.register < last_level) {
      current_level.register = parseInt(current_level.register) + 1;
      register(current_level.register);
    } else {
      yaz = await db('public.levels').where('id', '=', 1).update({ register: current_level.register });
      console.log(`son_register_level:${current_level.register}`);
      // mint();
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.register, state: 'register' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function burn() {
  try {
    const burn_index = await axios.get(`https://api.tzkt.io/v1/operations/transactions?level=${current_level.burn}&target.in=${FA2_CONTRACT}&entrypoint.eq=burn&select=parameter`);
    if (burn_index.data.length !== 0) {
      for (var i = 0; i < burn_index.data.length; i++) {
        try {
          try {
            const results = await db.transaction(async (trx) => {
              await trx.raw('SET CONSTRAINTS ALL DEFERRED;');
              await trx('public.holders').decrement('amount', burn_index.data[i].value.amount).where('holder_address', '=', burn_index.data[i].value.address).andWhere('token_id', '=', burn_index.data[i].value.token_id)
                .transacting(trx);
              await trx('public.tokens').decrement('editions', burn_index.data[i].value.amount).increment('burned', burn_index.data[i].value.amount).where('token_id', '=', burn_index.data[i].value.token_id)
                .transacting(trx);
              console.log(`burn${burn_index.data[i].value.token_id}`);
            });
          } catch (e) {
            console.log(`burn hatasi${current_level.burn}`);
          }
        } catch (e) {
          console.log(`burn kayit hatasi:${current_level.burn}`);
          console.error(e);
        }
      }
      if (current_level.burn < last_level) {
        current_level.burn = parseInt(current_level.burn) + 1;
        burn(current_level.burn);
      } else {
        await db('public.levels').where('id', '=', 1).update({ burn: current_level.burn });
        console.log(`son_burn_level:${current_level.burn}`);
      }
    } else if (current_level.burn < last_level) {
      current_level.burn = parseInt(current_level.burn) + 1;
      burn(current_level.burn);
    } else {
      await db('public.levels').where('id', '=', 1).update({ burn: current_level.burn });
      console.log(`son_burn_level:${current_level.burn}`);
    }
  } catch (e) {
    try {
      await db('public.errors').insert({ level: current_level.burn, state: 'burn' });
    } catch (e) {
      console.error(e);
    }
  }
}

async function check_levels() {
  if (start == false) {
    await set_last_indexed_level();
  }
  try {
    last_level = await got('https://api.tzkt.io/v1/blocks/count', { timeout: { request: 3000 } }).json() - LAG_N_BLOCKS;
    console.log(`son kontrol: ${last_level}`);

    while (current_level < last_level) {
      await Promise.all([mint(), register(), transfer(), metadata_fixer()]);
    }
  } catch (e) {
    console.log('check_levels hatasi');
    console.error(e);
  }
}

cron.schedule('*/30 * * * * * ', () => {
  check_levels();
});
