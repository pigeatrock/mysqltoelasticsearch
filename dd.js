const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

// elasticsearch bluk
async function run() {
    const dataset = [{
        id: 6,
        text: 'If I fall, don\'t bring me back.',
        user: 'jon',
        date: new Date()
    }]

    const body = dataset.flatMap(doc => [{ index: { _index: 'companydd', _id: 6 } }, doc])
    const { body: bulkResponse } = await client.bulk({ refresh: true, body })

    if (bulkResponse.errors) {
        const erroredDocuments = []
        // The items array has the same order of the dataset we just indexed.
        // The presence of the `error` key indicates that the operation
        // that we did for the document has failed.
        bulkResponse.items.forEach((action, i) => {
            const operation = Object.keys(action)[0]
            if (action[operation].error) {
                erroredDocuments.push({
                    // If the status is 429 it means that you can retry the document,
                    // otherwise it's very likely a mapping error, and you should
                    // fix the document before to try it again.
                    status: action[operation].status,
                    error: action[operation].error,
                    operation: body[i * 2],
                    document: body[i * 2 + 1]
                })
            }
        })
        console.log(erroredDocuments)
    }

    const { body: count } = await client.count({ index: 'companydd' })
    console.log(count)
}

run().catch(console.log)