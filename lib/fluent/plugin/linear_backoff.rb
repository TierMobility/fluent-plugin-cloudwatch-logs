def backoff(max_retries, sleep_dividend, description, *args)
    flg_retry = true
    flg_retry_count = 0
    while flg_retry do
        begin
            response = yield(*args)
            flg_retry = false
        rescue Exception => e
            log.warn("#{description}: #{flg_retry_count} #{e}")
            flg_retry_count += 1
            if flg_retry_count == max_retries
                log.error("#{description}: Max retry limit reached quiting #{e}")
                return nil
            else
                sleep flg_retry_count/sleep_dividend
            end
        end
    end
    response
end