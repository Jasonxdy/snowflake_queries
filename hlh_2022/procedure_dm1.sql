CREATE OR REPLACE PROCEDURE HLHOLDINGS.ODS.SP_DM1()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    
    try {
        const date = new Date();

        // Get current year, month, day
        //let day = date.getDate();
        //let month = date.getMonth() + 1;
        //let year = date.getFullYear();
        let day = ''3'';
        let month = ''10''
        let year = ''2022'';
        
        // 해당 월의 first business day 확인
        var query_get_business_day = "select business_day from hlholdings.public.monthly_business_days where year="+year+" and month="+month+"order by BUSINESS_DAY asc limit 1";
        var statement1 = snowflake.createStatement( {sqlText: query_get_business_day} );
        var result_set1 = statement1.execute();
        
        // 첫번째 ROW는 SKIP
        result_set1.next();
        
        // 쿼리에서 반환된 business_day 값 추출
        var first_business_day = result_set1.getColumnValue(1);
        
        var return_string = "NULL";
        
        // 오늘 날짜가 first_business_day인지 확인
        if(first_business_day == day) {
            // prepend ''0'' to day and month if required
            day = String(day);
            month = String(month);
            if(day.toString().length == 1) {
                day = ''0'' + day;
            }
            if(month.toString().length == 1) {
                month = ''0'' + month
            }
            
            var creation_date = String(year) + month + day;
            
            var sql_command="INSERT INTO HLHOLDINGS.MART.DM1 \\
            SELECT \\
                CREATION_DATE, \\
                A.MSCODE, \\
                B.DEALER_NAME, \\
                ITEM_CODE, \\
                AFFILIATION_GBN, \\
                TOTAL_ORDER_QTY, \\
                BO_QTY, \\
                HAIMS_QTY, \\
                ITS_QTY, \\
                CUST_COUNT, \\
                SALE_PRICE, \\
                BO_AMOUNT, \\
                DMD_DATE, \\
                AG_STSCD, \\
                AS_STSNM, \\
                ITEM_TYPE01, \\
                ITEM_TYPE02, \\
                ITEM_TYPE03, \\
                ITEM_NAME, \\
                WORK_SEQ, \\
                A.INS_DT, \\
                ALLOC_QTY, \\
                CDM_CODNM, \\
                SWRE_DMD_TYPE \\
            FROM \\
                MOBIS_BACKORDER_INFO_TEMP_LOG A, \\
                DEALER_INFO B \\
            WHERE \\
                A.CREATION_DATE = ''" + creation_date + "'' \\
                AND A.WORK_SEQ in (select max(work_seq) \\
                    FROM MOBIS_BACKORDER_INFO_TEMP_LOG where CREATION_DATE = ''"+creation_date+"'') \\
                AND A.MSCODE = B.MSCODE";
            
            snowflake.execute({sqlText: sql_command});
        
        
            return_string = "SUCCESS | HLHOLDINGS.ODS.SP_DM1() | CREATION_DATE=" + creation_date;
        } else {
            return_string = "SUCCESS | SKIP: NOT the first business day of the month.";
        }
        
        
        return return_string;
      
    } catch(err) {
    
        return "FAIL | HLHOLDINGS.ODS.SP_DM1() | " + err;
    }
    
  ';