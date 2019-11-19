//package com.sinaif.stream.operator;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import com.sinaif.stream.common.model.Event;
//import com.sinaif.stream.common.model.operator.UserCall;
//import com.sinaif.stream.common.model.operator.UserCallKeySelection;
//import com.sinaif.stream.operator.datainput.OperatorDataHandler.EventMapper;
//
//public class TestSocketFlinkEvent {
//
//	public static void main(String[] args) throws Exception {
//
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		DataStream<Event> dataStream = env.socketTextStream("localhost", 9999)
//				.map(EventMapper.INSTANCE)
//				.filter((a)-> a!=null)
//				.keyBy(UserCallKeySelection.SELECTIOR)
//				.timeWindow(Time.minutes(1))
//				.reduce((target, event) -> {
//				
//					UserCall targetCall = (UserCall) target.playload;
//					UserCall eventCall = (UserCall) event.playload;
//					targetCall.use_time += eventCall.use_time;
//					targetCall.subtoal += eventCall.subtoal;
//					targetCall.callTimes += 1;
//					targetCall.analysisByHour(eventCall);
//
//					// TODO more
//					return target;
//
//				});
////		dataStream.print();
//		
//		dataStream.addSink(new KuduSaverSink()).name("check_value") // task name
//				.setParallelism(4); // task parallelism, here means 5 threads
//		
//		env.execute("Window WordCount");
//	}
//	
//	public static class KuduSaverSink extends RichSinkFunction<Event> {
//		
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void invoke(Event value, Context context) throws Exception {
//			
//			System.out.println(value);
//		}
//	}
//
//	
//}