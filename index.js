const fs = require('fs')
    , path = require('path')

const join = path.join

module.exports.run = async function({
  db,
  path = join(process.cwd(), 'migrations'),
  debug = false,
  table = 'migrations',
  before = null,
  after = null
}) {
  debug && require('pg-monitor').attach(db.$config.options)
  const migrations = fs.readdirSync(path)
    .filter(x => fs.statSync(join(path, x)).isDirectory() && x.match(/^[0-9]{5}_/))
    .sort()
    .map(x => ({
      path: join(path, x),
      migration_id: parseInt(x.slice(0, 5)),
      name: x.slice(6).replace(/-/g, ' ')
    }))

  const latest = migrations[migrations.length - 1]

  if (latest.migration_id !== migrations.length)
    throw new Error('Inconsistency in migration numbering')

  return ensureMigrationsTable().then(next).then(() => migrations.slice(latest.migration_id))

  async function next() {
    const current = await getCurrentMigration()
        , nextMigration = migrations[current.migration_id]

    if (!nextMigration)
      return

    before && before(nextMigration)
    await run(nextMigration)
    after && after(nextMigration)
    await next()
  }

  function run({
    path,
    migration_id,
    name
  }) {
    return db.tx({
      mode: new db.$config.pgp.txMode.TransactionMode({
        tiLevel: db.$config.pgp.txMode.isolationLevel.serializable
      })
    }, async function(tx) {
      const load = file => new db.$config.pgp.QueryFile(join(path, file), { minify: true })

      fs.existsSync(join(path, 'index.sql')) && !fs.existsSync(join(path, 'index.js'))
        ? await tx.any(load('index.sql'))
        : await require(path)({ tx, pgp: db.$config.pgp, load }) // eslint-disable-line

      await tx.none(`
        insert into $(table:name) (
          migration_id,
          name
        ) values (
          $(migration_id),
          $(name)
        )
      `, {
        migration_id,
        name,
        table
      })
    })
  }

  function getCurrentMigration() {
    return db.oneOrNone(`
      select migration_id from $(table:name)
      order by migration_id desc
      limit 1
    `, {
      table
    }).then(migration => migration || { migration_id: 0 })
  }

  function ensureMigrationsTable() {
    return db.none(`
      create table if not exists $(table:name) (
        migration_id serial primary key,
        created_at timestamp with time zone not null default now(),
        name text
      )
    `, {
      table
    })
  }

}
