libname ct '/wrds/nyse/sasdata/taqms/ct';
/*libname cq '/wrds/nyse/sasdata/taqms/cq';*/
libname worklib '/home/umd/jingyili/realized_variance';

%macro trades_realized_variance(start, n_years, date, n_days);

%do i=1 %to &n_years;
        %let i_year = %eval(&start+&i-1);
%do j=1 %to &n_days;
        %let j_day = %eval(&date+&j-1);

data trades_&i_year.&j_day;
	set ct.ctm_&i_year.&j_day (keep = DATE TIME_M SYM_ROOT EX TR_CORR TR_SCOND SIZE PRICE);
	where TIME_M between '09:30:00't and '16:00:00't and SIZE > 0 and PRICE > 0 and TR_CORR = '00';
	if TR_SCOND in (' ', '@');
/* TR_SCOND: Sales Condition (@ = Regular Trade, ' ' = Regular Trade (no associated conditions);
TR_CORR: Trade Correction Indicator (00 = Regular trade which was not corrected, changed or signified as cancel or error.*/
run;

proc sql noprint;
  select distinct SYM_ROOT into: ticker_list_&i_year.&j_day separated by ' ' from trades_&i_year.&j_day;
run;

/* checking TR_SCOND indexing
proc freq data=trades_&i_year.&j_day noprint;
        table TR_SCOND / out=TR_SCOND_&i_year.&j_day;
proc print data = TR_SCOND_&i_year.&j_day;
*/

/* checking data
proc print data = trades_&i_year.&j_day (obs = 10);
*/

/* create time interval variables*/
data trades_t_&i_year.&j_day;
	set trades_&i_year.&j_day (keep = DATE TIME_M SYM_ROOT EX SIZE PRICE);
	diff_time = time_m - '09:30:00't;
	array new(10) interval_min_1-interval_min_10;
	%do t = 1 %to 10;
		interval_min_&t = CEIL((diff_time - mod(diff_time, %eval(&t) * 60)) / (%eval(&t) * 60));
	%end;
/*	%do t = 1 %to 10;
	array new_(10) interval_sec_1-interval_sec_10;
		interval_sec_&t = CEIL((diff_time - mod(diff_time, %eval(&t))) / %eval(&t));
	%end; */
run;

/*proc print data = trades_t_&i_year.&j_day (obs = 30);*/

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
	RV_min_&k._&i_year.&j_day = (PRICE_log_lag - PRICE_log)**2;

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

data trades_rv_&i_year.&j_day;
	set trades_rv_min_1_&i_year.&j_day;

%do t = 2 %to 10;

data trades_rv_&i_year.&j_day;
	merge trades_rv_&i_year.&j_day trades_rv_min_&t._&i_year.&j_day;
	by DATE SYM_ROOT;

%end;

proc print data = trades_rv_&i_year.&j_day (obs = 10);

data trades_rv_&i_year.&j_day;
	set trades_rv_&i_year.&j_day;
	array diff(9) rv_diff_2-rv_diff_10;
        %do l = 2 %to 10;
		rv_diff_&l = RV_min_1_&i_year.&j_day._sum - RV_min_&l._&i_year.&j_day._sum;
	%end;

/*proc print data = trades_rv_&i_year.&j_day(obs = 10);*/

proc summary data = trades_rv_&i_year.&j_day;
             class DATE SYM_ROOT;
             var rv_diff_2 rv_diff_3 rv_diff_4 rv_diff_5 rv_diff_6 rv_diff_7 rv_diff_8 rv_diff_9;
                 ways 2;
             output out=trades_rv_diff_&i_year.&j_day sum= /autoname;

proc print data = trades_rv_diff_&i_year.&j_day(obs = 10);

/*
%do t = 2 %to 9;
title '&i_year.&j_day RV difference between &t minutes' interval and 1 minute's interval';
proc univariate data = trades_rv_diff_&i_year.&j_day noprint;
	var rv_diff_&t._Sum;
	histogram rv_diff_&t._Sum / BARLABEL = COUNT;
run;
%end;
*/

proc univariate data = trades_rv_diff_&i_year.&j_day noprint;
	var rv_diff_2_Sum;
	histogram rv_diff_2_Sum / BARLABEL = COUNT;
run;

/*
filename output ’rv_diff_2_Sum.pdf’;
proc univariate data = trades_rv_diff_&i_year.&j_day noprint;
        var rv_diff_2_Sum rv_diff_3_Sum rv_diff_4_Sum rv_diff_5_Sum rv_diff_6_Sum rv_di$
        histogram rv_diff_2_Sum / BARLABEL = COUNT;
run;

ods graphics on/ DISCRETEMAX = 1300;

%do t = 2 %to 9;
proc freq data = trades_rv_diff_&i_year.&j_day noprint;
        table rv_diff_&t._Sum / out = trades_rv_&t._diff_freq_&i_year.&j_day;

proc print data = trades_rv_&t._diff_freq_&i_year.&j_day (obs = 10);

data trades_rv_&t.__diff_freq_&i_year.&j_day;
        set trades_rv_&t._diff_freq_&i_year.&j_day;
        if percent ge 0.01;

proc print data = trades_rv_&t._diff_freq_&i_year.&j_day (obs = 10);


proc sgplot data = trades_rv_&t.__diff_freq_&i_year.&j_day;
        vbar rv_diff_&t._Sum /response = percent datalabel;
        xaxis label ='Difference of Realized Variance';
        yaxis label ='Percentage %';
        title "&i_year.&j_day RV difference between &t minutes' interval and 1 minute's interval";

%end;

ods graphics off / DISCRETEMAX = 1300;
*/

%end;
%end;

%mend trades_realized_variance;

%trades_realized_variance(2017,1,1017,1);

run;
