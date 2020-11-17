const fs = require('fs')
const mysql = require('mysql')
// const request = require('request')
// const maria = require('mariasql');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

const connection = mysql.createConnection({
    host: '127.0.0.1',
    user: 'root',
    password: '',
    database: 'qichacha'
})
connection.connect()

// connection.query('SELECT * FROM qichacha LIMIT 10', function (error, results, fields) {
//     if (error) throw error;
//     console.log('The solution is: ', results);
//   });

//   connection.end();

// const c = new maria({
//     host: '127.0.0.1',
//     user: 'root',
//     password: '',
//     db: 'qichacha'
// });

// c.query('SELECT * FROM qichacha LIMIT', {}, (err, rows) => {
//     if(err) {
//         throw err;
//     }
//     console.log(rows)
// })

var breakPoint = {
    id: 0, //同步的mysql数据库数据id
    limit: 500, //一次查询的数据条数
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
    // 查询elasticsearch
    // const { body } = await client.search({
    //     index: 'companysearch',
    //     body: {
    //         query: {
    //             match: {
    //                 name: '广州'
    //             }
    //         }
    //     }
    // })

    // console.log(body.hits.hits)

    // 批量导入数据
    while (isCon) {
        console.log('新一轮数据导入开始')

        //查询mysql数据库语句
        var queryDbSql = `SELECT id,name,status,oper_name,regist_capi,start_date,sheng,shi,xian,country_id,phone,email,trade_id,trade FROM qichacha WHERE id > ${breakPoint.id} LIMIT ${breakPoint.limit}`
        console.log(queryDbSql)
        let dbData = await queryDb(queryDbSql)
        // 写入elasticearch
        for (let i = 0; i < dbData.length; i++) {
            const element = dbData[i];
            client.create({
                index: 'companysss',
                id: element['id'],
                body: element
            })
            breakPoint.id++;
        }
        // 当查询完毕
        if (dbData.length < breakPoint.limit) {
            isCon = false
            // breakPoint.id += dbData.length
        } else {
            // 更新id
            // breakPoint.id += breakPoint.limit
        }
        saveBreakPoint('正常更新断点：' + JSON.stringify(breakPoint))

        await timeout()
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