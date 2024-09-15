CREATE OR REPLACE PROCEDURE `tactile-reason-430717-g4.nyc_stagging.agg_nyc_data`()
BEGIN
  DECLARE v_runtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_status STRING DEFAULT 'SUCCESS';
  DECLARE v_error_msg STRING DEFAULT NULL;

  BEGIN
    -- Populating the aggregate tables
    -- Insert aggregated data into the AggregatedPayroll table
    INSERT INTO `tactile-reason-430717-g4.nyc_prod2.AggregatedPayroll`
    SELECT 
        FiscalYear,
        CAST(AgencyID AS INT64) AS AgencyID,
        ROUND(SUM(BaseSalary), 2) AS TotalBaseSalary,
        ROUND(SUM(RegularGrossPaid), 2) AS TotalRegularGrossPaid,
        ROUND(SUM(TotalOTPaid),2) AS TotalOTPaid,
        ROUND(SUM(TotalOtherPay), 2) AS TotalOtherPay
    FROM 
        `tactile-reason-430717-g4.nyc_stagging.nyc_payrolldf`
    GROUP BY 
        FiscalYear, 
        AgencyID;

    -- Log the outcome
    INSERT INTO `tactile-reason-430717-g4.nyc_stagging.procedure_log` (run_time, status, error_message)
    VALUES (v_runtime, v_status, v_error_msg);

  EXCEPTION WHEN ERROR THEN
    -- Capture the error message
    SET v_status = 'FAILED';
    SET v_error_msg = ERROR_MESSAGE();

    -- Log the error
    INSERT INTO `tactile-reason-430717-g4.nyc_stagging.procedure_log` (run_time, status, error_message)
    VALUES (v_runtime, v_status, v_error_msg);

  END;
END;
