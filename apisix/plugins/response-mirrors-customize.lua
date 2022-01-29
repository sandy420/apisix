--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local ngx = ngx
local core = require("apisix.core")
local producer = require("resty.kafka.producer")

--local pairs = pairs
local plugin_name = "response-mirrors-customize"
local file_extension_list = { 'ico', 'css', 'js"', 'jsp', 'html', 'jpg', 'jpeg', "gif", 'png' }
local schema = {
    type = "object",
    properties = {
        prefer_name = {
            type = "boolean",
            default = true
        }
    },
}

local _M = {
    version = 0.1,
    priority = 99,
    name = plugin_name,
    schema = schema,
    run_policy = 'prefer_route',
}

function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)
    if not ok then
        return false, err
    end
    return true
end

function _M.destroy()
    -- call this function when plugin is unloaded
end

function string.starts(String, Start)
    return string.sub(String, 1, string.len(Start)) == Start
end

function string.ends(String, End)
    return End == '' or string.sub(String, -string.len(End)) == End
end

local function hold_body_chunk(ctx, hold_the_copy)
    local body_buffer
    local arg = ngx.arg
    local chunk, eof = arg[1], arg[2]
    if eof then
        local concat_tab = table.concat
        body_buffer = ctx._body_buffer
        core.log.info("message_info1_body_buffer is:", body_buffer)
        if not body_buffer then
            return chunk
        end

        body_buffer = concat_tab(body_buffer, "", 1, body_buffer.n)
        ctx._body_buffer = nil
        return body_buffer
    end

    if type(chunk) == "string" and chunk ~= "" then
        body_buffer = ctx._body_buffer
        if not body_buffer then
            body_buffer = {
                chunk,
                n = 1
            }
            ctx._body_buffer = body_buffer
        else
            local n = body_buffer.n + 1
            body_buffer.n = n
            body_buffer[n] = chunk
        end
    end

    if not hold_the_copy then
        -- flush the origin body chunk
        arg[1] = nil
    end
    return nil
end

local function new_collect_body(ctx)
    local final_body = hold_body_chunk(ctx, true)
    if not final_body then
        return
    end
    ctx.resp_body = final_body
    core.log.info('message_info2_final_body is:', final_body)
    return final_body
end

function _M.body_filter(ctx)
    new_collect_body(ctx)
end

local function payload_data(ctx)
    local log_json = {}
    log_json["host"] = ngx.var.host
    log_json["uri"] = ngx.var.uri
    core.log.info('message_info3_resp_body is:', ctx.resp_body)
    if ctx.resp_body ~= nil then
        log_json["response"] = ctx.resp_body
    end
    local message = core.json.encode(log_json)
    if not message then
        return false, 'error occurred while encoding the data. '
    end
    return message
end

local brokerList = {
    { host = "10.13.67.100", port = 9092 },
}
local TOPIC = 'test'

local function send_data_to_kfk(message)
    --封装response body数据发送至KFK
    local bp = producer:new(brokerList, { producer_type = "async" })
    core.log.info('message_info4_brokerList is:', core.json.encode(brokerList))
    core.log.info('message_info_message is5:', message)
    local ok, err = bp:send(TOPIC, nil, message)
    if not ok then
        return false, "failed to send data to Kafka topic: " .. err .. ", brokers: " .. core.json.encode(brokerList)
    else
        core.log.info('message_info_message is11:', err)
    end
    core.log.info('message_info6_ok is:', ok)
    return true
end

local function strSplit(delimeter, str)
    --用指定字符串切割另一个字符串
    local find, sub, insert = string.find, string.sub, table.insert
    local res = {}
    local start, start_pos, end_pos = 1, 1, 1
    while true do
        start_pos, end_pos = find(str, delimeter, start, true)
        if not start_pos then
            break
        end
        insert(res, sub(str, start, start_pos - 1))
        start = end_pos + 1
    end
    insert(res, sub(str, start))
    return res
end

local function is_include(value, tab)
    for _, v in ipairs(tab) do
        if v == value then
            return true
        end
    end
    return false
end

function _M.log(ctx)
    local message = payload_data(ctx)
    if message then
        local _uri = string.lower(ngx.var.uri)
        if string.match(_uri, '.') then
            local split_uri = strSplit('.', _uri)
            local uri_end = split_uri[#split_uri]
            core.log.info('message_info7_uri_end is:', uri_end)
            local _status = is_include(uri_end, file_extension_list)
            core.log.info('message_info8_status is:', _status)
            if _status ~= true then
                send_data_to_kfk(core.json.encode(message))
                core.log.info('message_info9_allend is:', core.json.encode(message))
            end
        else
            send_data_to_kfk(core.json.encode(message))
            core.log.info('message_info10_allend is:', core.json.encode(message))
        end
    end
end
return _M
