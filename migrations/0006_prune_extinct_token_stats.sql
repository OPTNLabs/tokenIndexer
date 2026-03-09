BEGIN;

DELETE FROM token_stats
WHERE total_ft_supply = 0
  AND holder_count = 0
  AND utxo_count = 0;

COMMIT;
