DELETE FROM answers;
DELETE FROM logs;
DELETE FROM nattn;
DELETE FROM queue;
DELETE FROM requests;
DELETE FROM skipped;
UPDATE system SET req_nattn_data=0, req_ttn_data=0;