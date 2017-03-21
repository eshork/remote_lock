require 'remote_lock/adapters/base'

module RemoteLock::Adapters
  class Redis < Base

    def incr(key, expires_in_seconds)
      r = @connection.incr(key) until(r and r.to_i > 0)
      @connection.expire(key, expires_in_seconds)
    end

    def decr(key)
      @connection.decr(key)
    end

    def is_positive?(key)
      r = @connection.get(key)
      (r and r.to_i > 0)
    end

    def store(key, expires_in_seconds)
      # The previous implementation used SETNX and EXPIRE in sequence to set the
      # lock. in case a previous client failed between SETNX and EXPIRE below,
      # the key may not expire.
      # We wrap setting the value and its expiry timestamp in a transaction.
      #
      # Caveat emptor: Redis transactions are *very* different from SQL
      # transactions.

      # cancel the next transaction if another client touches our key past
      # this point
      @connection.watch(key)

      # check if another client has the key.
      # it's important to still run a transaction to clear the watch.
      have_competition = @connection.exists(key)

      !! @connection.multi do
        break if have_competition
        @connection.setex(key, expires_in_seconds, uid)
      end
    end

    def delete(key)
      @connection.del(key)
    end

    def has_key?(key)
      @connection.get(key) == uid
    end

  end
end
