//package com.sinaif.stream.operator;
//
//import java.io.InputStream;
//import java.util.Properties;
//
//import org.python.core.Py;
//import org.python.core.PyFunction;
//import org.python.core.PyInteger;
//import org.python.core.PyObject;
//import org.python.core.PyString;
//import org.python.core.PySystemState;
//import org.python.util.PythonInterpreter;
//
///**
// *
// *
// */
//public class KeyGenerator {
//
//	public static void main(String[] args) {
//		
//
//		Properties props = new Properties();
////		String root_python = "/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7";
//		String root_python = "/Users/simonzhang/env/lib/python2.7/";
//		String root_site_path="/Users/simonzhang/env/lib/python2.7/site-packages";
////		String root_python = "/Users/simonzhang/jython2.7.0/";
////		String root_site_path="/Users/simonzhang/jython2.7.0/site-packages";
//		props.put("python.home", root_python);
//
//		props.put("python.console.encoding", "UTF-8");
//		props.put("python.security.respectJavaAccessibility", "false");
//		props.put("python.import.site", "false");
//		Properties preprops = System.getProperties();
//
//		PythonInterpreter.initialize(preprops, props, new String[0]);
//
//		PythonInterpreter interpreter = new PythonInterpreter();
//		PySystemState sys = Py.getSystemState();
//		sys.path.append(new PyString(root_python));
//		sys.path.append(new PyString(root_site_path));
////		sys.
//		sys.path.append(new PyString("/Users/simonzhang/env/lib/python2.7/lib-dynload/"));
//		
//
////		InputStream inputStream = KeyGenerator.class.getResourceAsStream("encryption.py");
////		interpreter.execfile("/Python27.programs/my_utils.py");
//		interpreter.execfile("/Users/simonzhang/workspace/ws/stream.operator/src/main/resources/encryption.py");
//		PyFunction func = (PyFunction) interpreter.get("phone_2pk", PyFunction.class);
//
//		String phone = "13928437712";
//		long start = System.currentTimeMillis();
//		PyObject pyobj = func.__call__(new PyString(phone));
//		System.out.println("anwser = " + pyobj.toString() + " time:" + (System.currentTimeMillis() - start));
//	}
//
//}
