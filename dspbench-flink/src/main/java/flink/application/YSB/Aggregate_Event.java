package flink.application.YSB;

public class Aggregate_Event {
	public String cmp_id;
	public long count;
	public long timestamp;

	public Aggregate_Event() {
		cmp_id = "";
		count = 0;
		timestamp = 0;
	}

	public Aggregate_Event(String _cmp_id, long _count, long _ts) {
		cmp_id = _cmp_id;
		count = _count;
		timestamp = _ts;
	}
}
