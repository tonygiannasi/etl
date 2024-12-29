#!/usr/bin/perl -w
use cPanelUserConfig;
############################################################
# Title: Extract Transform Load Program for Database Project
# Author: Tony Giannasi
# Last modified on 10/30/2020 2:07PM
# Copyright 2020 GEA, LLC. All Rights Reserved				    
############################################################
use DBI;
use DBD::mysql;
use CGI::Cookie;
use CGI;
use CGI::Carp qw ( fatalsToBrowser );
use Date::Calc qw(Day_of_Year Add_Delta_Days Delta_Days Week_of_Year Calendar);

# CONFIG VARIABLES
$platform = "mysql";
$database = "<db>";
$host = "localhost";
$port = "3306";
$user = "<user>";
$pw = '<passwd>';
$send_mail = 0;
$test = 1;
$q=new CGI;

$DEBUG = 1;

$dsn = "dbi:mysql:$database:localhost:3306";
$connect = DBI->connect($dsn, $user, $pw) or print "Unable to connect: $DBI::errstr\n";

$table = qq~<table class=inline border=1 width="100%" cellpadding=4 cellspacing=0 bordercolor=#AAAAAA style="border-left-width: 0px; border-right-width: 0px; border-collapse: collapse; border-top-style: solid; border-top-width: 1px; border-bottom-style: solid; border-bottom-width: 1px; padding-left: 4px; padding-right: 4px; padding-top: 1px; padding-bottom: 1px">~;

$|++;
$Title = "Tony's ETL Program";
print "Content-type: text/html\n\n";
print &head;
&initialize;
&etl;
&footer;
exit;

############################################################

sub etl {
	$showrows = 15;
	&dimplantloc($showrows);
	&dimplant($showrows);
	&dimissue($showrows);
	&factactiveplant($showrows);
	&factyieldloss($showrows);
	
	print "<h2>ETL COMPLETE</h2>";
}

sub dimplantloc {
	my $showrows = shift;
	my $missing = "<font color=red><B>MISSING DATA</B></font>";
	
	#D_PLANTLOC EXTRACT
	undef $dirty, $rowct;
	print "<h2>D_PLANTLOC Dimension Table</h2>";
	print $table;
	print qq~<tr bgcolor=#AAAAAA><td>boxid</td><td>actlocation</td></tr>\n~;
	$query = "SELECT boxid, actlocation FROM ACTIVEPLANT";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$boxid, \$actlocation);
	while($query_handle->fetch()) {
		if ($boxid eq '' || int($boxid) != $boxid) {$dirty = 1;$boxid = $missing;}
		if ($actlocation eq '' || int($actlocation) != $actlocation) {$dirty = 1;$actlocation = $missing;}
		($dirty, $boxid) = &int_test($dirty, $boxid);
		($dirty, $actlocation) = &int_test($dirty, $actlocation);
		$rowct++;
		if ($rowct <= $showrows) {print "<tr><td class=slick>$boxid</td><td class=slick>$actlocation</td></tr>\n";}
	}
	if ($dirty != 1) {
		print qq~<tr bgcolor=#AAAAAA><td colspan=2><b>DATA PASSED INTEGRITY CHECK</b></td></tr></table>~;
		}
	else {
		print qq~<tr bgcolor=#AAAAAA><td colspan=2><b><font color=red>DATA FAILED INTEGRITY CHECK - EXITING.</font></b></td></tr></table></body></html>~;
		exit;
		}
		
	#D_PLANTLOC TRANSFORM
	print qq~<h3>No transformation necessary</h3>~;
	
	#D_PLANTLOC LOAD: If data is complete, load it into Dimension table
	if ($dirty != 1) {
		$query = "INSERT INTO D_PLANTLOC (boxid, actlocation) SELECT boxid, actlocation FROM ACTIVEPLANT";
		if ($DEBUG) {print "<font color=green>$query</font><BR>";}
		$query_handle = $connect->prepare($query);
		$query_handle->execute();
		if ( $query_handle->err )
			{
			print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
			   <br><font color=blue>$query</font><BR>\n";
			exit;
			}
		else {print "<h3>Load Complete</h3>";}
		}
	
}

sub dimplant {
	my $showrows = shift;
	
	#D_PLANT EXTRACT
	undef $dirty;
	undef $rowct;
	print "<h2>D_PLANT Dimension Table</h2>";
	print $table;
	print qq~<tr bgcolor=#AAAAAA><td>plantid</td>
								 <td>plantname</td>
								 <td>plantgermdays</td>
								 <td>plantfertdays</td>
								 <td>plantharvdays</td>
								 <td>planttemplimit</td>
								 <td>plantsunneeded</td>
								 <td>planth20needed</td>
								 <td>plantcost</td>
								 <td>plantsaleprice</td>
								 <td>classname</td>
								 <td>classtype</td></tr>\n~;
	$query = "SELECT plantid, plantname, plantgermdays, plantfertdays, plantharvdays, planttemplimit, plantsunneeded, planth20needed, plantcost, plantsaleprice, classname, classtype 
	FROM PLANT p, PLANTCLASS c 
	WHERE p.classid = c.classid";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$plantid, \$plantname, \$plantgermdays, \$plantfertdays, \$plantharvdays, \$planttemplimit, \$plantsunneeded, \$planth20needed, \$plantcost, \$plantsaleprice, \$classname, \$classtype);
	while($query_handle->fetch()) {
		($dirty, $plantid) = &int_test($dirty, $plantid);
		($dirty, $plantname) = &str_test($dirty, $plantname);
		($dirty, $plantgermdays) = &int_test($dirty, $plantgermdays);
		($dirty, $plantfertdays) = &int_test($dirty, $plantfertdays);
		($dirty, $plantharvdays) = &int_test($dirty, $plantharvdays);
		($dirty, $planttemplimit) = &int_test($dirty, $planttemplimit);
		($dirty, $plantsunneeded) = &int_test($dirty, $plantsunneeded);
		($dirty, $planth20needed) = &int_test($dirty, $planth20needed);
		($dirty, $plantcost) = &float_test($dirty, $plantcost);
		($dirty, $plantsaleprice) = &float_test($dirty, $plantsaleprice);
		($dirty, $classname) = &str_test($dirty, $classname);
		($dirty, $classtype) = &str_test($dirty, $classtype);
		$rowct++;
		if ($rowct <= $showrows) {print "<tr><td class=slick>$plantid</td>
											 <td class=slick>$plantname</td>
											 <td class=slick>$plantgermdays</td>
											 <td class=slick>$plantfertdays</td>
											 <td class=slick>$plantharvdays</td>
											 <td class=slick>$planttemplimit</td>
											 <td class=slick>$plantsunneeded</td>
											 <td class=slick>$planth20needed</td>
											 <td class=slick>$plantcost</td>
											 <td class=slick>$plantsaleprice</td>
											 <td class=slick>$classname</td>
											 <td class=slick>$classtype</td></tr>\n";}
	}
	if ($dirty != 1) {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b>DATA PASSED INTEGRITY CHECK</b></td></tr></table>~;
		}
	else {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b><font color=red>DATA FAILED INTEGRITY CHECK - EXITING.</font></b></td></tr></table></body></html>~;
		exit;
		}
		
	#D_PLANT TRANSFORM
	print qq~<h3>No transformation necessary</h3>~;
	
	#D_PLANT LOAD: If data is complete, load it into Dimension table
	if ($dirty != 1) {
		$query = "INSERT INTO D_PLANT (plantid, plantname, plantgermdays, plantfertdays, plantharvdays, planttemplimit, plantsunneeded, planth20needed, plantcost, plantsaleprice, classname, classtype) 
	SELECT plantid, plantname, plantgermdays, plantfertdays, plantharvdays, planttemplimit, plantsunneeded, planth20needed, plantcost, plantsaleprice, classname, classtype 
	FROM PLANT p, PLANTCLASS c 
	WHERE p.classid = c.classid";
		if ($DEBUG) {print "<font color=green>$query</font><BR>";}
		$query_handle = $connect->prepare($query);
		$query_handle->execute();
		if ( $query_handle->err )
			{
			print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
			   <br><font color=blue>$query</font><BR>\n";
			exit;
			}
		else {print "<h3>Load Complete</h3>";}
		}
	
}

sub dimissue {
	my $showrows = shift;
		
	#D_ISSUE EXTRACT
	undef $dirty;
	undef $rowct;
	print "<h2>D_ISSUE Dimension Table</h2>";
	print $table;
	print qq~<tr bgcolor=#AAAAAA><td>issid</td>
								 <td>isstype</td>
								 <td>issname</td>
								 <td>isstreatcost</td></tr>\n~;
	$query = "SELECT issid, isstype, issname, isstreatcost FROM ISSUE";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$issid, \$isstype, \$issname, \$isstreatcost);
	while($query_handle->fetch()) {
		($dirty, $issid) = &int_test($dirty, $issid);
		($dirty, $isstype) = &str_test($dirty, $isstype);
		($dirty, $issname) = &str_test($dirty, $issname);
		($dirty, $isstreatmentcost) = &float_test($dirty, $isstreatcost);
		$rowct++;
		if ($rowct <= $showrows) {print "<tr><td class=slick>$issid</td>
											 <td class=slick>$isstype</td>
											 <td class=slick>$issname</td>
											 <td class=slick>$isstreatcost</td></tr>\n";}
	}
	if ($dirty != 1) {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b>DATA PASSED INTEGRITY CHECK</b></td></tr></table>~;
		}
	else {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b><font color=red>DATA FAILED INTEGRITY CHECK - EXITING.</font></b></td></tr></table></body></html>~;
		exit;
		}
		
	#D_ISSUE TRANSFORM
	print qq~<h3>No transformation necessary</h3>~;
	
	#D_ISSUE LOAD: If data is complete, load it into Dimension table
	if ($dirty != 1) {
		$query = "INSERT INTO D_ISSUE (issid, isstype, issname, extracost) 
	SELECT issid, isstype, issname, isstreatcost 
	FROM ISSUE";
		if ($DEBUG) {print "<font color=green>$query</font><BR>";}
		$query_handle = $connect->prepare($query);
		$query_handle->execute();
		if ( $query_handle->err )
			{
			print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
			   <br><font color=blue>$query</font><BR>\n";
			exit;
			}
		else {print "<h3>Load Complete</h3>";}
		}
	
}

sub factactiveplant {
	my $showrows = shift;
		
	#F_ACTIVEPLANT EXTRACT
	undef $dirty;
	undef $rowct;
	print "<h2>F_ACTIVEPLANT Fact Table</h2>";
	print $table;
	print qq~<tr bgcolor=#AAAAAA><td>calendarkey</td>
								 <td>plantlockey</td>
								 <td>issuekey</td>
								 <td>trackdate</td>
								 <td>plantkey</td>
								 <td>tracknote</td></tr>\n~;
	$query = "SELECT DISTINCT calendarkey, plantlockey, issuekey, trackdate, plantkey, tracknote 
	FROM TRACKEDIN t, ACTIVEPLANT a, D_CALENDAR dc, D_PLANTLOC dpl, D_PLANT dp, D_ISSUE di 
	WHERE t.trackdate = dc.fulldate AND 
		a.activeid = t.activeid AND 
		a.boxid = dpl.boxid AND 
		a.actlocation = dpl.actlocation AND 
		a.plantid = dp.plantid AND 
		t.issid = di.issid";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$calendarkey, \$plantlockey, \$issuekey, \$trackdate, \$plantkey, \$tracknote);
	while($query_handle->fetch()) {
		($dirty, $calendarkey) = &int_test($dirty, $calendarkey);
		($dirty, $plantlockey) = &int_test($dirty, $plantlockey);
		($dirty, $issuekey) = &int_test($dirty, $issuekey);
		($dirty, $trackdate) = &date_test($dirty, $trackdate);
		($dirty, $plantkey) = &int_test($dirty, $plantkey);
		($dirty, $tracknote) = &str_test($dirty, $tracknote);
		$rowct++;
		if ($rowct <= $showrows) {print "<tr><td class=slick>$calendarkey</td>
											 <td class=slick>$plantlockey</td>
											 <td class=slick>$issuekey</td>
											 <td class=slick>$trackdate</td>
											 <td class=slick>$plantkey</td>
											 <td class=slick>$tracknote</td></tr>\n";}
	}
	if ($dirty != 1) {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b>DATA PASSED INTEGRITY CHECK</b></td></tr></table>~;
		}
	else {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b><font color=red>DATA FAILED INTEGRITY CHECK - EXITING.</font></b></td></tr></table></body></html>~;
		exit;
		}
		
	#F_ACTIVEPLANT TRANSFORM
	print qq~<h3>No transformation necessary</h3>~;
	
	#F_ACTIVEPLANT LOAD: If data is complete, load it into Fact table
	if ($dirty != 1) {
		$query = "INSERT INTO F_ACTIVEPLANT (calendarkey, plantlockey, issuekey, issuedate, plantkey, issuedata) 
	SELECT DISTINCT calendarkey, plantlockey, issuekey, trackdate, plantkey, tracknote 
	FROM TRACKEDIN t, ACTIVEPLANT a, D_CALENDAR dc, D_PLANTLOC dpl, D_PLANT dp, D_ISSUE di 
	WHERE t.trackdate = dc.fulldate AND 
		a.activeid = t.activeid AND 
		a.boxid = dpl.boxid AND 
		a.actlocation = dpl.actlocation AND 
		a.plantid = dp.plantid AND 
		t.issid = di.issid;";
		if ($DEBUG) {print "<font color=green>$query</font><BR>";}
		$query_handle = $connect->prepare($query);
		$query_handle->execute();
		if ( $query_handle->err )
			{
			print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
			   <br><font color=blue>$query</font><BR>\n";
			exit;
			}
		else {print "<h3>Load Complete</h3>";}
		}
	
}

sub factyieldloss {
	my $showrows = shift;
		
	#F_YIELDLOSS EXTRACT
	undef $dirty;
	undef $rowct;
	print "<h2>F_YIELDLOSS Fact Table</h2>";
	print $table;
	print qq~<tr bgcolor=#AAAAAA><td>calendarkey</td>
								 <td>plantlockey</td>
								 <td>issuekey</td>
								 <td>trackdate</td>
								 <td>plantkey</td>
								 <td>tracknote</td></tr>\n~;
								 
	$query = "SELECT activeid, SUM(extracost) FROM TRACKEDIN GROUP BY activeid";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$activeid, \$sumcost);
	while($query_handle->fetch()) {
		$sumcost{$activeid} = $sumcost;
		}
	
	$query = "SELECT a.activeid, calendarkey, plantlockey, edate, dp.plantsaleprice, dp.plantcost, e.edata FROM 
	PLANTEVENT e, ACTIVEPLANT a, D_CALENDAR dc, D_PLANTLOC dpl, D_PLANT dp WHERE 
		e.edate = dc.fulldate AND 
		a.activeid = e.activeid AND 
		a.boxid = dpl.boxid AND 
		a.actlocation = dpl.actlocation AND 
		a.plantid = dp.plantid AND 
		(e.edata = 'Put out for Sale' OR e.edata = 'Removed and destroyed')";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$activeid, \$calendarkey, \$plantlockey, \$edate, \$plantsaleprice, \$plantcost, \$edata);
	while($query_handle->fetch()) {
		if (uc($edata) eq 'PUT OUT FOR SALE') {$yieldloss = $plantsaleprice - $plantcost - $sumcost{$activeid};}
		else {$yieldloss = 0 - $plantcost - $sumcost{$activeid};}
		
		($dirty, $calendarkey) = &int_test($dirty, $calendarkey);
		($dirty, $plantlockey) = &int_test($dirty, $plantlockey);
		($dirty, $trackdate) = &date_test($dirty, $edate);
		($dirty, $yieldloss) = &float_test($dirty, $yieldloss);
		$rowct++;
		if ($rowct <= $showrows) {print "<tr><td class=slick>$calendarkey</td>
											 <td class=slick>$plantlockey</td>
											 <td class=slick>$edate</td>
											 <td class=slick>$yieldloss</td></tr>\n";}
		$sql{$rowct} = "INSERT INTO F_YIELDLOSS VALUES ($calendarkey, $plantlockey, '$edate', $yieldloss)";
	}
	if ($dirty != 1) {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b>DATA PASSED INTEGRITY CHECK</b></td></tr></table>~;
		}
	else {
		print qq~<tr bgcolor=#AAAAAA><td colspan=12><b><font color=red>DATA FAILED INTEGRITY CHECK - EXITING.</font></b></td></tr></table></body></html>~;
		exit;
		}
		
	#F_YIELDLOSS TRANSFORM
	print qq~<h3>Sum of Costs aggregated into a hash table with Active ID as the key, then yield or loss calculated from what event occured (PUT OUT FOR SALE / REMOVED AND DESTROYED)</h3>~;
	
	#F_YIELDLOSS LOAD: If data is complete, load it into Fact table
	if ($dirty != 1) {
		foreach (keys %sql) {
			$query = $sql{$_};
			if ($DEBUG) {print "<font color=green>$query</font><BR>";}
			$query_handle = $connect->prepare($query);
			$query_handle->execute();
			if ( $query_handle->err )
				{
				print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
				   <br><font color=blue>$query</font><BR>\n";
				exit;
				}
			}
		print "Load Complete<BR>";
		}
}


sub int_test {
	my $missing = "<font color=red><B>MISSING DATA</B></font>";
	my $invalid = "<font color=red><B>BAD FORMAT DATA</B></font>";
	my $d = shift;
	my $v = shift;
	if ($v eq '') {$d = 1;$v = $missing;}
	if (int($v) != $v) {$d = 1;$v = $invalid;}
	return ($d, $v);
}

sub float_test {
	my $missing = "<font color=red><B>MISSING DATA</B></font>";
	my $invalid = "<font color=red><B>BAD FORMAT DATA</B></font>";
	my $d = shift;
	my $v = shift;
	if ($v eq '') {$d = 1;$v = $missing;}
	if ($v*1 != $v) {$d = 1;$v = $invalid;}
	return ($d, $v);
}

sub str_test {
	my $missing = "<font color=red><B>MISSING DATA</B></font>";
	my $invalid = "<font color=red><B>BAD FORMAT DATA</B></font>";
	my $d = shift;
	my $v = shift;
	if ($v eq '') {$d = 1;$v = $missing;}
	if ($v =~ /^[\p{Alnum}\s]{0,30}\z/) {} else {$d = 1;$v = $v."<br>".$invalid;}
	return ($d, $v);
}

sub date_test {
	my $missing = "<font color=red><B>MISSING DATA</B></font>";
	my $invalid = "<font color=red><B>BAD FORMAT DATA</B></font>";
	my $d = shift;
	my $v = shift;
	if ($v eq '') {$d = 1;$v = $missing;}
	if ($v =~ /^[0-9\-]+$/) {} else {$d = 1;$v = $v."<br>".$invalid;}
	return ($d, $v);
}

sub initialize {
	
	$query = "DELETE FROM F_ACTIVEPLANT";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	if ( $query_handle->err )
		{
		print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
		   <br><font color=blue>$query</font><BR>\n";
		exit;
		}
		
	$query = "DELETE FROM F_YIELDLOSS";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	if ( $query_handle->err )
		{
		print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
		   <br><font color=blue>$query</font><BR>\n";
		exit;
		}
	
	$query = "DELETE FROM D_PLANTLOC";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	if ( $query_handle->err )
		{
		print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
		   <br><font color=blue>$query</font><BR>\n";
		exit;
		}
	
	$query = "DELETE FROM D_PLANT";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	if ( $query_handle->err )
		{
		print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
		   <br><font color=blue>$query</font><BR>\n";
		exit;
		}
	
	$query = "DELETE FROM D_ISSUE";
	if ($DEBUG) {print "<font color=green>$query</font><BR>";}
	$query_handle = $connect->prepare($query);
	$query_handle->execute();
	if ( $query_handle->err )
		{
		print "<hr><font color=red>DBI ERROR! : ".$query_handle->err." : ".$query_handle->errstr." </font>
		   <br><font color=blue>$query</font><BR>\n";
		exit;
		}
}

sub head {
return qq~<html><head><title>$Title</title>
<style>
td {
    font-family: "Gill Sans MT";
    color: #5C4033;
    font-size: 10pt;
	padding:5px;
 }
 
.slick {
	font-size: 12pt;
	border-left-width: 0px; 
	border-right-width: 0px; 
	border-top-style: solid; 
	border-top-width: 1px; 
	border-bottom-style: solid; 
	border-bottom-width: 1px;
 }
.slick2 {
	font-size: 10pt;
	border-left-width: 0px; 
	border-right-width: 0px; 
	border-top-width: 0px; 
	border-bottom-width: 0px;
 }
table.slick {
	border-spacing: 10px;
	border-collapse: collapse;
	}
th {
    font-family: "Gill Sans MT";
    color: #5C4033;
    font-size: 8pt;
 }

body {
    font-family: "Gill Sans MT";
    color: #5C4033;
    font-size: 10pt;
	margin: 0;
    padding: 0;
 }
</style>
</head><body>~;
}

sub footer {
print qq~</body></html>~;
}
