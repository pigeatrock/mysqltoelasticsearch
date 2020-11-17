const fs = require('fs')
const mysql = require('mysql')

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

const connection = mysql.createConnection({
    host: '127.0.0.1',
    user: 'root',
    password: '',
    database: 'qichacha'
})
connection.connect()

var breakPoint = {
    id: 0, //同步的mysql数据库数据id
    limit: 5000, //一次查询的数据条数
};

var isCon = true;

// 初始化断点
try {
    fs.accessSync('./breakpoint', fs.constants.R_OK | fs.constants.W_OK);
    var breakPointTmp = fs.readFileSync('./breakpoint', { encoding: 'utf-8' })
    if (breakPointTmp) {
        breakPoint = JSON.parse(breakPointTmp)
    }
    console.log('读取断点文件成功');
} catch (err) {
    console.error('没有断点文件');
    fs.appendFileSync('./breakpoint', '{}');
    console.log('生成断点文件成功');
}

//监听程序退出
process.on('SIGINT', async () => {
    console.log('程序退出')
    isCon = false
    saveBreakPoint('程序正常退出')
    process.exit();
});

//延时函数
function timeout(ms = 1000) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function run() {
    // 批量导入数据
    while (isCon) {
        console.log('新一轮数据导入开始')

        //查询mysql数据库语句
        var queryDbSql = `SELECT id,name,status,oper_name,regist_capi,start_date,sheng,shi,xian,country_id,phone,email,trade_id,trade FROM qichacha WHERE id > ${breakPoint.id} LIMIT ${breakPoint.limit}`
        console.log(queryDbSql)
        let dbData = await queryDb(queryDbSql)

        if (dbData) {
            // 写入elasticearch（用bulk)
            let body = dbData.flatMap(doc => [{ index: { _index: 'companyss', _id: doc['id'] } }, doc])
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
            // 当查询完毕
            // 更新id
            breakPoint.id += dbData.length
            saveBreakPoint('正常更新断点：' + JSON.stringify(breakPoint))
            if (dbData.length < breakPoint.limit) { //数据查询完毕终止查询
                isCon = false
            }

            const { body: count } = await client.count({ index: 'companyss' })
            console.log(count)
            await timeout()
        }
    }
}

// 查询数据mysql
function queryDb(querySql) {
    return new Promise((resolve, reject) => {
        connection.query(querySql, (error, results, fields) => {
            if (error) {
                console.log('[QUERY ERROR] - ', error.message);
                saveBreakPoint('数据查询出错')
                reject(error)
                throw '数据查询出错'
                return;
            } else {
                resolve(results)
            }
        })
    })
}

//记录日志
function log(msg = '') {
    let time = new Date()
    time += '\n'
    time += msg
    time += '\n------------------------------\n'
    fs.appendFileSync('log.txt', time)
}

//记录断点
function saveBreakPoint(msg = '') {
    fs.writeFileSync('./breakpoint', JSON.stringify(breakPoint))
    log('保存断点' + msg)
}

run().catch(console.log)

// connection.end();