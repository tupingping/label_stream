package com.sinaif.stream.kudu.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * for kudu column
 * 
 * @author simonzhang
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KuduColumn {

	public String property() default "";
	
	public String type() default "";
	
	public boolean autochange() default false;
	
}
