const express = require('express')
const app = express()
const bodyParser = require('body-parser')
const { Client } = require('@elastic/elasticsearch')

const {
  RECLASTIC_NODE,
  RECLASTIC_INDEX,
  RECLASTIC_FIELD
} = process.env

const client = new Client({ node: RECLASTIC_NODE })

const serviceMetadata = {
  name: 'Reconciliation for Elasticsearch',
  identifierSpace: `${RECLASTIC_NODE}/${RECLASTIC_INDEX}`,
  schemaSpace: `${RECLASTIC_NODE}/${RECLASTIC_INDEX}`,
  view: {
    url: `${RECLASTIC_NODE}/${RECLASTIC_INDEX}/_all/{{id}}/_source`
  },
}

app.use(bodyParser.urlencoded({ extended: true }))

app.get('/', (req, res) => {
  res.send(`${req.query.callback}(${JSON.stringify(serviceMetadata)})`)
})

app.post('/', async (req, res) => {
  const queries = Object.entries(JSON.parse(req.body.queries))
  const keys = queries.map(([name]) => name)
  const response = (await Promise.all(
    queries.map(([name, { query }]) => client.search({
      index: RECLASTIC_INDEX,
      body: { query: { match: { [RECLASTIC_FIELD]: { query, _name: name } } } }
    }))
  )).reduce((acc, { body: { hits: { hits } } }, i) => Object.assign(acc, {
    [keys[i]]: {
      result: hits.map(hit => ({
        id: hit._id,
        type: [hit._type],
        name: hit._source[RECLASTIC_FIELD],
        score: hit._score,
        match: false
      }))
    }
  }), {})

  console.log(JSON.stringify(response, null, 2))
  res.send(response)
})

app.listen(3000, () => {
  console.log('Reclastic app listening on port 3000!')
})
