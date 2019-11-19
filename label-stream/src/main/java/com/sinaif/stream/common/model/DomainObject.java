package com.sinaif.stream.common.model;

import java.util.Date;

import com.sinaif.stream.kudu.connector.KuduColumn;

public class DomainObject implements AutoChange{

	
	@KuduColumn(property = "creat_dt")
	public Date creat_dt;
	
	@KuduColumn(property = "upDate_dt",  autochange=true)
	public Date upDate_dt;

	@Override
	public void autochange() {
		upDate_dt = new Date();
	}
	
	
}
