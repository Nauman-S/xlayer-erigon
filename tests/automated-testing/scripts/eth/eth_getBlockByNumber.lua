dofile("common.lua")
methodName = "eth_getBlockByNumber"
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

threads = {}
counter = 1

setup = function(thread)
    thread:set("t_id", counter)
    counter = counter + 1
    thread:set("counter_invalid", 0)
    thread:set("counter_valid", 0)
    thread:set("counter_failed", 0)
    table.insert(threads, thread)
    math.randomseed(os.time() + counter)
end

request = function()
    local block = math.random(1, 11276923) -- Supports 16 threads, 20,000 address parameters
    local fullTx = (math.random(0, 1) == 1)
    local body = string.format('{"jsonrpc":"2.0","method":"%s","params":["0x%X",%s],"id":1}', methodName, block, tostring(fullTx))
    headers = {}
    headers["Content-Type"] = "application/json"
    return wrk.format("POST", nil, headers, body)
end

response = function(status, headers, body)
    local counter_invalid = wrk.thread:get("counter_invalid")
    local counter_valid = wrk.thread:get("counter_valid")
    local counter_failed = wrk.thread:get("counter_failed")
    if string.find(body, '"error":') then
        counter_invalid = counter_invalid + 1
        -- print(1, body, "  ", "SET ", counter)
    elseif string.find(body, '"result":') then
        counter_valid = counter_valid + 1
        -- print(2, body)
    elseif not string.find(body, '"jsonrpc":') then
        counter_failed = counter_failed + 1
    end
    wrk.thread:set("counter_invalid", counter_invalid)
    wrk.thread:set("counter_valid", counter_valid)
    wrk.thread:set("counter_failed", counter_failed)
end

done = function(summary, latency, requests)
    print_summary(summary, latency, requests, threads)
end

