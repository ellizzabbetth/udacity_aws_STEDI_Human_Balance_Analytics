SELECT COUNT(*) FROM customer_trusted WHERE shareWithResearchAsOfDate IS NOT NULL AND shareWithResearchAsOfDate <> ''; --482
SELECT COUNT(*) FROM accelerometer_trusted; --40981

select count(*) from customer_curated -- Count of customer_curated: 482 rows
select count(*) from machine_learning_curated -- Count of machine_learning_curated: 43681 rows
select count(*) from trainer_trusted --14460