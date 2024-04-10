package flink.source;

import java.io.Serializable;

// CampaignAd class
public class CampaignAd implements Serializable
{
	public String ad_id;
	public String campaign_id;

    // constructor
    public CampaignAd()
    {
    	ad_id = null;
    	campaign_id = null;
    }

    // constructor
    public CampaignAd(String _ad_id, String _compagin_id)
    {
    	ad_id = _ad_id;
    	campaign_id = _compagin_id;
    }
}
