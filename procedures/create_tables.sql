CREATE TABLE IF NOT EXISTS `tactile-reason-430717-g4.nyc_prod2.AggregatedPayroll` (
        FiscalYear INT,
        AgencyID INT,
        TotalBaseSalary FLOAT64,
        TotalRegularGrossPaid FLOAT64,
        TotalOTPaid FLOAT64,
        TotalOtherPay FLOAT64
    );
	
	
CREATE TABLE `tactile-reason-430717-g4.nyc_stagging.procedure_log` (
    id INT64,
    run_time TIMESTAMP NOT NULL,
    status STRING(50) NOT NULL,
    error_message STRING
);
