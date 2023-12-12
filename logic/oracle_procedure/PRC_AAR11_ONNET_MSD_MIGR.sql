CREATE OR REPLACE PROCEDURE CDIP_IN.PRC_AAR11_ONNET_MSD_MIGR
IS
l_count number;
l_procedure_name VARCHAR2(100);
BEGIN

l_procedure_name := $$PLSQL_UNIT;

-- Reset of previous runs
UPDATE CDIP_OUT.T_GP_FINAL_AZ_2310_V103
SET "FLAG_ONNET_OHNE_BT"= 0,
"FLAG_ONNET" = 0,
"FLAG_ONNET_BT" = 0
;
commit;

MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 final
USING (
        select address_key
         from DATALAB.ONNET_ADRESSEN_GESAMT
        ) ivon 
ON (final."ADDRESS_KEY"     =   ivon."ADDRESS_KEY")
WHEN MATCHED THEN
    UPDATE SET 
        final."FLAG_ONNET"= 1
        ;

l_count := SQL%ROWCOUNT;
COMMIT;

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count Onnet Gesamt: ' || l_count);
COMMIT;


MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 final
USING (
        select address_key
         from DATALAB.ONNET_ADRESSEN_OHNE_BT
        ) ivon 
ON (final."ADDRESS_KEY"     =   ivon."ADDRESS_KEY")
WHEN MATCHED THEN
    UPDATE SET 
        final."FLAG_ONNET_OHNE_BT"= 1
        ;

l_count := SQL%ROWCOUNT;
COMMIT;

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count Onnet ohne BT: ' || l_count);
COMMIT;


MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 final
USING (
        select address_key
         from DATALAB.ONNET_ADRESSEN_NUR_BT
        ) ivon 
ON (final."ADDRESS_KEY"     =   ivon."ADDRESS_KEY")
WHEN MATCHED THEN
    UPDATE SET 
        final."FLAG_ONNET_BT"= 1
        ;

l_count := SQL%ROWCOUNT;
COMMIT;

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count Onnet nur BT: ' || l_count);
COMMIT;


END;