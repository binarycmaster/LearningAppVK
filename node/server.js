var express = require('express');
var dbcon = require("./db-pool.json");
var mysql = require('mysql');
const crypto = require('crypto');
var app = express();

app.use(function (req, res, next) {
    res.header('Access-Control-Allow-Origin', '*:*');
    res.header('Access-Control-Allow-Methods', 'GET, OPTIONS', 'POST');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Credentials', true);
    return next();
});
const cors = require('cors');
app.use(cors({
    origin: '*'
}));
var Promise = require('promise');
var server = require('http').createServer(app);
server.listen(3000);
//server.timeout = 1000;
var io = require('socket.io')(server, {
    allowEIO3: true,
    pingTimeout: 120000,
    maxHttpBufferSize: 1e8,
    cors: {
        origin: "*",
        methods: ["GET", "POST", "OPTIONS"]
    }
});
//io.set('origins', '*:*');
const path = require('path');
const moment = require('moment-timezone');
const dbpool = require('mysql').createPool(dbcon);
//moment().tz.setDefault("Asia/Kolkata");
moment.tz.setDefault("Asia/Kolkata");
app.get('/', function (req, res) {
    res.sendFile(path.join(__dirname + '/admin.html'));
});
const request = require('request');
var con;
var client_conn = [];
var timearr = [];
var last_check = 0;
var scrips_last_update = 0;
var kite_token = "PC8n55hZjdzy1OTJNrziPEb9j0i9dNup";
var kite_api_key = "act1301jv6rvkvae";
var last_conn_try = 0;
var ticker;
var scrip_id_arr = {};
var zerodha_connected = 0;


function zerodha_connection() {
    var time = new Date().getTime();
    if (time < last_conn_try - 5000)
        return;
    last_conn_try = time;
    var KiteTicker = require("kiteconnect").KiteTicker;
    ticker = new KiteTicker({
        api_key: kite_api_key,
        access_token: kite_token,
    });
    ticker.autoReconnect(false, 10, 5);
    ticker.connect();
    ticker.on('disconnect', onDisconnect);
    ticker.on('error', onError);
    ticker.on('close', onClose);
    ticker.on("ticks", onTicks);
    ticker.on("connect", function () {
        console.log("connected");
        zerodha_connected = 1;
        add_scrips();
    });

    ticker.on("noreconnect", function () {
        console.log("noreconnect");
    });

    ticker.on("reconnecting", function (reconnect_interval, reconnections) {
        console.log(
                "Reconnecting: attempt - ",
                reconnections,
                " innterval - ",
                reconnect_interval
                );
    });
}
// set autoreconnect with 10 maximum reconnections and 5 second interval


function onTicks(ticks) {
  //  console.log(ticks);
    //console.log(JSON.stringify(ticks));
    var values = [];
    var questions = [];
    var emits = [];
    var current_time = new Date().getTime() / 1000;
    var update_diff = current_time - scrips_last_update;
    if (update_diff > 60)
        scrips_last_update = current_time;
    for (var i = 0; i < ticks.length; i++) {
        if ('instrument_token' in ticks[i]) {
            var zerodha_instrument_token = ticks[i].instrument_token;
            var scrip_id = scrip_id_arr[zerodha_instrument_token];
            var bid = 0;
            var ask = 0;
            if ('depth' in ticks[i]) {
                var depth = ticks[i].depth;
                if ('buy' in depth) {
                    var bidArr = depth.buy;
                    var askArr = depth.sell;

                    var bid = bidArr[0]["price"];
                    var ask = askArr[0]["price"];
                }
            }
            var ltp = 0;
            var atp = 0;
            var ltq = 0;
            var volume = 0;
            var prev_close = 0;
            var oi = 0;
            var prev_open_int_close = 0;
            var day_turnover = 0;
            var special = "";
            var bid_qty = 0;
            var ask_qty = 0;
            var formatted_time = ticks[i].exchange_timestamp;
            if (formatted_time == null)
                return;
            if ('ohlc' in ticks[i]) {
                var ohlc = ticks[i].ohlc;
                var open = ohlc.open;
                var low = ohlc.low;
                var high = ohlc.high;
                var prev_close = ohlc.close;
            }
            if ('last_price' in ticks[i])
                ltp = ticks[i].last_price;
            if ('average_traded_price' in ticks[i])
                atp = ticks[i].average_traded_price;
            if ('last_traded_quantity' in ticks[i])
                ltq = ticks[i].last_traded_quantity;
            if ('volume_traded' in ticks[i])
                volume = ticks[i].volume_traded;
            if ('oi' in ticks[i])
                oi = ticks[i].oi;
            if ('Prev_Open_Int_Close' in ticks[i])
                prev_open_int_close = ticks[i].Prev_Open_Int_Close;
            if ('Day_Turnover' in ticks[i])
                day_turnover = ticks[i].Day_Turnover;
            if ('Special' in ticks[i])
                special = ticks[i].Special;
            if ('total_buy_quantity' in ticks[i])
                bid_qty = ticks[i].total_buy_quantity;
            if ('total_sell_quantity' in ticks[i])
                ask_qty = ticks[i].total_sell_quantity;

            var ms_time = new Date(formatted_time).getTime();
            if (ms_time != 'NaN') {
                var time = parseInt(ms_time / 1000);
                var current_ms_time = new Date().getTime();
                questions.push('(?,?,?,?, ?,?,?,?)');
                values.push(scrip_id, bid, ask, time, high, low, current_ms_time, ltp);
                var custom_data = {Bid: bid, Ask: ask, Symbol: scrip_id, Open: open, High: high, Low: low, LTP: ltp, ATP: atp, Prev_Close: prev_close, OI: oi, Bid_Qty: bid_qty, Ask_Qty: ask_qty, Volume: volume, LTQ: ltq, active_clients: io.engine.clientsCount};
		if(scrip_id=='ALUMINI23MAYFUT')
		 console.log(custom_data);
                emits.push(custom_data);
                if (update_diff > 60)
                {
		    if(scrip_id=='ALUMINI23MAYFUT')
                           console.log('test'+custom_data);

                  //  var update_values = [atp, open, ltq, volume, prev_close, oi, prev_open_int_close, day_turnover, special, bid_qty, ask_qty, scrip_id]; // Commented by Murugan
		     var update_values = [bid, ask, atp, open, ltq, volume, prev_close, oi, prev_open_int_close, day_turnover, special, bid_qty, ask_qty, scrip_id];
                    update_scrips(update_values);
                }
            }
        }

    }
    if (questions.length > 0)
        add_to_db(questions.join(), values, emits);
    if (update_diff > 10)
        update_connected_clients();
    //console.log("Ticks", ticks);
    //add_to_db(ticks, "tick");
}

function update_connected_clients() {
    var sql = "UPDATE `admin` set `connected_clients`=?";

    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(update_connected_clients, 1000);
        } else {
            connection.query(sql, [io.engine.clientsCount], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    //Do nothing
                }
            });
        }
    });
}

function update_scrips(values) {


//    var sql = "UPDATE `scrips` set `atp`=?, `open`=?, `ltq`=?, `volume`=?, `prev_close`=?, `oi`=?, `prev_open_int_close`=?, `day_turnover`=?, `special`=?, `bid_qty`=?, `ask_qty`=? where scrip_id=?";  //  Commented by Murugan
   var sql = "UPDATE `scrips` set `bid_price`=?, `ask_price`=?, `atp`=?, `open`=?, `ltq`=?, `volume`=?, `prev_close`=?, `oi`=?, `prev_open_int_close`=?, `day_turnover`=?, `special`=?, `bid_qty`=?, `ask_qty`=? where scrip_id=?";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                update_scrips(values);
            }, 1000);
        } else {
            connection.query(sql, values, (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    //Do nothing
                }
            });
        }
    });
}

function onDisconnect(error) {
    zerodha_connected = 0;
    var time = new Date().getTime();
    console.log("disconnect at " + time + " " + last_conn_try);
    if (time < last_conn_try - 2000)
        return;
    setTimeout(function () {
        zerodha_connection();
    }, 5000);
    // console.log("Closed connection on disconnect", error);
}

function onError(error) {
    console.log("Closed connection on error", error);
    var time = new Date().getTime();
    if (time < last_conn_try - 2000)
        return;
    ticker.disconnect();
    //console.log("Closed connection on error", error);
}

function onClose(reason) {
    console.log("Closed connection on close", reason);
    var time = new Date().getTime();
    if (time < last_conn_try - 2000)
        return;
    ticker.disconnect();
    //console.log("Closed connection on close", reason);
}
function zerodha_access_token() {
    if (zerodha_connected == 1)
        return;
    var sql = "select zerodha_api_key, zerodha_access_token from admin";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(zerodha_access_token, 1000);
        } else {
            connection.query(sql, [], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    kite_token = String(res[0].zerodha_access_token);
                    kite_api_key = String(res[0].zerodha_api_key);
                }
            });
        }
    });
}


io.on('connection', function (client) {
    var time = moment().tz("asia/kolkata").unix();
    var handshakeData = client.request;
    var response = handshakeData._query['value'];
    var obj = JSON.parse(response);
    io.to(client.id).emit("db_err", {message: "Recvd." + response});
    if ('username' in obj) {
        var username = obj.username;
        var password = obj.password;
        var role = obj.role;
        check_login(username, password, role, client);
    } else if ('userid' in obj) {
        var userid = obj.userid;
        var security_code = obj.security_code;
        if (userid != null)
            validate_login(userid, security_code, client);
    } else if ('admin_id' in obj) {
        var userid = obj.admin_id;
        var security_code = obj.security_code;
        validate_admin_login(userid, security_code, client);
    } else if ('broker_id' in obj) {
        var userid = obj.broker_id;
        var security_code = obj.security_code;
        validate_broker_login(userid, security_code, client);
    } else {
        client.disconnect();
    }

    client.on("subscribe_symbol", function (data) {
        client.join(data.room);
        io.to(client.id).emit("symbol_subscribed", {message: client.id + "Symbol Subscribed " + data.room});
    });
    client.on("unsubscribe_symbol", function (data) {
        client.leave(data.room);
        io.to(client.id).emit("symbol_unsubscribed", {message: client.id + " Symbol UnSubscribed " + data.room});
    });
});


function check_login(username, password, role, client) {
    io.to(client.id).emit("db_err", {message: "in check login"});
    var hash = crypto.createHash('sha256').update(password).digest('hex');
    var sql = "select id from users where username=? and password=? and user_type=? and status=1";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                check_login(username, password, role, client);
            }, 1000);
        } else {
            connection.query(sql, [username, hash, role], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    io.to(client.id).emit("db_err", {message: "query started"});
                    if (res.length == 0) {
                        io.to(client.id).emit("invalid_login", {message: "Please Enter Correct Login Credentials"});
                        client.disconnect();
                    } else {
                        io.to(client.id).emit("db_err", {message: "data matched"});
                        var userid = res[0].id;
                        var security_code = random_str(15);
                        update_security_code(userid, security_code, client);
                    }
                }
            });
        }
    });
}

function validate_login(userid, security_code, client) {
    io.to(client.id).emit("db_err", {message: "in validate login"});
    var sql = "select id from app_session where userid=? and security_code=? and status=1";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                validate_login(userid, security_code, client);
            }, 1000);
        } else {
            connection.query(sql, [userid, security_code], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    if (res.length == 0) {
                        io.to(client.id).emit("invalid_login", {message: "Please Enter Correct Login Credentials"});
                        client.disconnect();
                    } else {
                        io.to(client.id).emit("login_successful", {userid: userid, security_code: security_code, app_id: res[0].id});
                        client.join("all_users");
                    }
                }
            });
        }
    });
}

function validate_broker_login(userid, security_code, client) {
    io.to(client.id).emit("db_err", {message: "in check login"});
    var sql = "select id from app_session where broker_id=? and security_code=? and status=1";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                validate_broker_login(userid, security_code, client);
            }, 1000);
        } else {
            connection.query(sql, [userid, security_code], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    if (res.length == 0) {
                        io.to(client.id).emit("invalid_login", {message: "Please Enter Correct Login Credentials"});
                        client.disconnect();
                    } else {
                        io.to(client.id).emit("login_successful", {userid: userid, security_code: security_code, app_id: res[0].id, usertype: "Broker"});
                        client.join("all_users");
                    }
                }
            });
        }
    });

}

function validate_admin_login(userid, security_code, client) {
    io.to(client.id).emit("db_err", {message: "in check login"});
    var sql = "select id from app_session where admin_id=? and security_code=? and status=1";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                validate_admin_login(userid, security_code, client);
            }, 1000);
        } else {
            connection.query(sql, [userid, security_code], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    if (res.length == 0) {
                        io.to(client.id).emit("invalid_login", {message: "Please Enter Correct Login Credentials"});
                        client.disconnect();
                    } else {
                        io.to(client.id).emit("login_successful", {userid: userid, security_code: security_code, app_id: res[0].id, usertype: "admin"});
                        client.join("all_users");
                    }
                }
            });
        }
    });

}


function remove_scrips() {
    var ms_time = new Date().getTime();
    var time = parseInt(ms_time / 1000);
    var scrips = [];
    var sql = "select zerodha_instrument_token, scrip_id from scrips where status=1 and expiry_time<?";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(remove_scrips, 1000);
        } else {
            connection.query(sql, [time], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    for (var i = 0; i < res.length; i++) {
                        scrips.push(res[i].zerodha_instrument_token);
                        update_scrip_status(res[i].scrip_id);
                    }
                    ticker.unsubscribe(scrips);
                }
            });
        }
    });
}
function update_scrip_status(scrip_id) {
    var sql = "update scrips set status=0 where status=1 and scrip_id=?";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                update_scrip_status(scrip_id);
            }, 1000);
        } else {
            connection.query(sql, [scrip_id], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    //Do nothing
                }
            });
        }
    });
}
function add_scrips() {
    var ms_time = new Date().getTime();
    var time = parseInt(ms_time / 1000);
    var scrips = [];
    var sql = "select zerodha_instrument_token, scrip_id from scrips where status=1 and expiry_time>?";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(add_scrips, 1000);
        } else {
            connection.query(sql, [time], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    for (var i = 0; i < res.length; i++) {
                        scrips.push(res[i].zerodha_instrument_token);
                        scrip_id_arr[res[i].zerodha_instrument_token] = res[i].scrip_id;
                    }
                    ticker.subscribe(scrips);
                    ticker.setMode(ticker.modeFull, scrips);
                }
            });
        }
    });

}
function random_str(length) {
    var result = '';
    var characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var charactersLength = characters.length;
    for (var i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() *
                charactersLength));
    }
    return result;
}
function update_security_code(userid, security_code, client) {
    var ms_time = new Date().getTime();
    var time = parseInt(ms_time / 1000);
    io.to(client.id).emit("db_err", {message: "updating security code"});
    var sql = "insert into app_session(userid,security_code,time) values(?,?,?)";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                update_security_code(userid, security_code, client);
            }, 1000);
        } else {
            connection.query(sql, [userid, security_code, time], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    io.to(client.id).emit("db_err", {message: "updating security code query started"});
                    var app_id = res.insertId;
                    if (app_id == 0 || app_id === false) {
                        io.to(client.id).emit("db_err", {message: "security code not added"});
                        return update_security_code(userid, security_code, client);
                    }
                    logout_other_devices(userid, app_id, client);
                    io.to(client.id).emit("login_successful", {userid: userid, security_code: security_code, app_id: app_id});
                    client.join("all_users");
                }
            });
        }
    });
}
function logout_other_devices(userid, app_id, client) {
    var sql = "update app_session set status=0 where id<>? and userid=?";
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                logout_other_devices(userid, app_id, client);
            }, 1000);
        } else {
            connection.query(sql, [app_id, userid], (errQuery, res) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    //Do nothing
                }
            });
        }
    });
}

function add_to_db(questions, values, emits) {

    var current_ms_time = new Date().getTime();
    var current_time = parseInt(current_ms_time / 1000);
    var sql = "INSERT INTO `scrip_data`( `scrip_id`, `bid_price`, `ask_price`, `time`, `high`, `low`, `recorded_time`, `ltp`) VALUES " + questions;
    dbpool.getConnection((err, connection) => {
        if (err) {
            console.log('error connecting. retrying in 1 sec');
            setTimeout(function () {
                add_to_db(questions, values, emits);
            }, 1000);
        } else {
            connection.query(sql, values, (errQuery, result) => {
                connection.release();
                if (errQuery) {
                    console.log('Error querying database!');
                } else {
                    if (result.insertId == 0 || result.insertId === false)
                        return;// add_to_db(data, type);
                    for (var i = 0; i < emits.length; i++) {
                        io.to("all_scrips").emit("scrip_data", {data: emits[i]});
                        io.to(emits[i].Symbol).emit("scrip_data", {data: emits[i]});
                    }
                    //Executed. Do anything else you have to do here
                    if (parseInt(current_time) - parseInt(last_check) > 60) {
                        last_check = current_time;
                        add_scrips();
                        remove_scrips();
                    }
                }
            });
        }
    });

}
zerodha_connection();
setInterval(function () {
    zerodha_access_token();
}, 2000);
