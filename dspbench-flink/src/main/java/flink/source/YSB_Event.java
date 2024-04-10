package flink.source;

public class YSB_Event {
	public String uuid1;
	public String uuid2;
	public String ad_id;
	public String ad_type;
	public String event_type;
	public long timestamp;
	public String ip;

	public YSB_Event() {
		uuid1 = "";
		uuid2 = "";
		ad_id = "";
		ad_type = "";
		event_type = "";
		timestamp = 0;
		ip = "";
	}

	public YSB_Event(String _uuid1, String _uuid2, String _ad_id, String _ad_type, String _event_type, long _ts, String _ip) {
		uuid1 = _uuid1;
		uuid2 = _uuid2;
		ad_id = _ad_id;
		ad_type = _ad_type;
		event_type = _event_type;
		timestamp = _ts;
		ip = _ip;
	}
}
