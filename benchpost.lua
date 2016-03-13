wrk.method = "POST"
wrk.body = "[{ \"eventType\": \"chat2\", \"data\": { \"foo\": \"bar\" } }]"
wrk.headers["Content-Type"] = "application/json"
