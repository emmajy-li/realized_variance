libname ct '/wrds/nyse/sasdata/taqms/ct';
/*libname cq '/wrds/nyse/sasdata/taqms/cq';*/
libname worklib '/home/umd/jingyili/realized_variance';

%macro trades_realized_variance(start, n_years, date, n_days);

%do i=1 %to &n_years;
        %let i_year = %eval(&start+&i-1);
%do j=1 %to &n_days;
        %let j_day = %eval(&date+&j-1);
/*
%if &j_day/1000 lt 1 %then %do;

data trades_&i_year.&j_day;
        set ct.ctm_&i_year.&j_day (keep = DATE TIME_M SYM_ROOT EX TR_CORR TR_SC$
        where TIME_M between '09:30:00't and '16:00:00't and SIZE > 0 and PRICE$
        if TR_SCOND in (' ', '@');
TR_SCOND: Sales Condition (@ = Regular Trade, ' ' = Regular Trade (no associ$
TR_CORR: Trade Correction Indicator (00 = Regular trade which was not corrected$
run;

%end;

%else %do;
*/

data trades_&i_year.&j_day;
	set ct.ctm_&i_year.0&j_day (keep = DATE TIME_M SYM_ROOT EX TR_CORR TR_SCOND SIZE PRICE);
	where TIME_M between '09:30:00't and '16:00:00't and SIZE > 0 and PRICE > 0 and TR_CORR = '00';
	if TR_SCOND in (' ', '@');
run;

/*
%end;
*/

proc sql noprint;
  select distinct SYM_ROOT into ticker_list_&i_year.&j_day separated by ' ' from trades_&i_year.&j_day;
run;

proc print data = ticker_list_&i_year.&j_day (obs = 10);

/* checking TR_SCOND indexing
proc freq data=trades_&i_year.&j_day noprint;
        table TR_SCOND / out=TR_SCOND_&i_year.&j_day;
proc print data = TR_SCOND_&i_year.&j_day;
*/

/* checking data
proc print data = trades_&i_year.&j_day (obs = 10);
*/

/* create time interval variables in minutes and seconds*/
data trades_t_&i_year.&j_day;
	set trades_&i_year.&j_day (keep = DATE TIME_M SYM_ROOT EX SIZE PRICE);
	diff_time = time_m - '09:30:00't;
	array min(10) interval_min_1-interval_min_10;
	%do t = 1 %to 10;
		interval_min_&t = CEIL((diff_time - mod(diff_time, %eval(&t) * 60)) / (%eval(&t) * 60));
	%end;
	%do t = 1 %to 10;
	array sec(10) interval_sec_1-interval_sec_10;
		interval_sec_&t = CEIL((diff_time - mod(diff_time, %eval(&t))) / %eval(&t));
	%end;
run;

proc print data = trades_t_&i_year.&j_day (obs = 30);
title 'Check time interval variables in minutes and seconds';

%do k = 1 %to 10;

data trades_min_&k._&i_year.&j_day;
	set trades_t_&i_year.&j_day (keep = DATE TIME_M SYM_ROOT interval_min_&k PRICE);

proc sort data = trades_min_&k._&i_year.&j_day out = trades_s_min_&k._&i_year.&j_day;
        by DATE SYM_ROOT interval_min_&k;

/*proc print data = trades_s_min_&k._&i_year.&j_day (obs = 10);*/

data trades_s_min_&k._&i_year.&j_day;
        set trades_s_min_&k._&i_year.&j_day (keep = DATE SYM_ROOT interval_min_&k PRICE);
	by DATE SYM_ROOT interval_min_&k;
        if last.interval_min_&k = 1;

/*proc print data = trades_s_min_&k._&i_year.&j_day (obs = 20);*/

data trades_s_min_&k._&i_year.&j_day;
	set trades_s_min_&k._&i_year.&j_day;
	PRICE_log = log(PRICE);
	PRICE_log_lag = lag(PRICE_log);
	if first.SYM_ROOT = 1 then PRICE_log_lag = .;
	RV_min_&k._&i_year.&j_day = ((PRICE_log - PRICE_log_lag))**2;

proc print data = trades_s_min_&k._&i_year.&j_day (obs = 10);

/*
%local h next_ticker;
%do h=1 %to %sysfunc(countw(&ticker_list_&i_year.&j_day));
   %let next_ticker = %scan(&ticker_list_&i_year.&j_day, &h);
   %proc print data=next_ticker;
%end;


%let h = 1;
%do %while (%scan(ticker_list_&i_year.&j_day, &h) ne );
	%let next_ticker = %scan(ticker_list_&i_year.&j_day, &h);
	%data trades_&next_ticker;
		%set trades_s_min_&k._&i_year.&j_day (where= (SYM_ROOT = &next_ticker));
	%proc print data = trades_&next_ticker (obs = 1);
	%let h = %eval(&h + 1);
%end;
*/
/*
proc means data = trades_s_min_&k._&i_year.&j_day noprint;
	var RV_min_&k._&i_year.&j_day;
	output out = _winsor1
	p1(RV_min_&k._&i_year.&j_day)= p99(RV_min_&k._&i_year.&j_day)= /autoname;

data trades_s_min_&k._&i_year.&j_day._w (drop= RV_min_&k._&i_year.&j_day._p1 RV_min_&k._&i_year.&j_day._p99);
	set trades_s_min_&k._&i_year.&j_day;
	by interval_min_&k;
	if _n_=1 then set _winsor3 (drop=_TYPE_ _FREQ_);
        if RV_min_&k._&i_year.&j_day ne . then RV_min_&k._&i_year.&j_day._w = max(RV_min_&k._&i_year.&j_day._p1, min(RV_min_&k._&i_year.&j_day._p99, RV_min_&k._&i_year.&j_day));
*/
 
proc summary data = trades_s_min_&k._&i_year.&j_day;
             class DATE SYM_ROOT;
             var RV_min_&k._&i_year.&j_day;
                 ways 2;
             output out=trades_rv_min_&k._&i_year.&j_day sum= /autoname;

/*proc print data = trades_rv_min_&k._&i_year.&j_day (obs = 10);*/

%end;

/* Concatenate data with different minute interval together*/
data trades_rv_&i_year.&j_day;
	set trades_rv_min_1_&i_year.&j_day;

%do t = 2 %to 10;

data trades_rv_&i_year.&j_day;
	merge trades_rv_&i_year.&j_day trades_rv_min_&t._&i_year.&j_day;
	by DATE SYM_ROOT;

%end;

proc print data = trades_rv_&i_year.&j_day (obs = 50);

%end;
%end;

%mend trades_realized_variance;

%trades_realized_variance(2015,1,0202,1);

run;
