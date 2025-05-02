dofile("common.lua")
methodName = "eth_getRawTransactionByHash"
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

threads = {}
counter = 1

setup = function(thread)
    thread:set("t_id",counter)
    counter = counter + 1
    thread:set("counter_invalid", 0)
    thread:set("counter_valid", 0)
    thread:set("counter_failed", 0)
    table.insert(threads,thread)
    math.randomseed(os.time()+counter)
end

request = function()
    local hash = "0x9fb8dfe1f6dd5d5e72c92237ee6d99af8814cddf31b786dfa6d674aabe965957"
    local fullTx = (math.random(0, 1) == 1)
    local body = string.format('{"jsonrpc":"2.0","method":"%s","params":["%s"],"id":1}', methodName, hash)
    -- print(body)
    headers = {}
    headers["Content-Type"] = "application/json"
    return wrk.format("POST",nil,headers,body)
end

response = function(status, headers, body)
    local counter_invalid = wrk.thread:get("counter_invalid")
    local counter_valid = wrk.thread:get("counter_valid")
    local counter_failed = wrk.thread:get("counter_failed")
    if string.find(body,'"error":') then
        counter_invalid = counter_invalid + 1
--         print("Error response")
--         print("Status: " .. status)
--         print("Body: " .. body)
        -- print(1,body,"  ", "SET ",counter)
    elseif string.find(body,'"result":') then
        counter_valid = counter_valid + 1
        -- print(2,body)
    elseif not string.find(body,'"jsonrpc":') then
        counter_failed = counter_failed + 1
--         print("RPC call failed")
--         print("Status: " .. status)
--         print("Body: " .. body)
    end
    wrk.thread:set("counter_invalid", counter_invalid)
    wrk.thread:set("counter_valid", counter_valid)
    wrk.thread:set("counter_failed", counter_failed)
end

done = function(summary, latency, requests)
    print_summary(summary, latency, requests, threads)
end