package flink.application.YSB;

public class Joined_Event {
	public String cmp_id;
	public String ad_id;
	public long timestamp;

	public Joined_Event() {
		cmp_id = "";
		ad_id = "";
		timestamp = 0;
	}

	public Joined_Event(String _cmp_id, String _ad_id, long _ts) {
		cmp_id = _cmp_id;
		ad_id = _ad_id;
		timestamp = _ts;
	}
}
