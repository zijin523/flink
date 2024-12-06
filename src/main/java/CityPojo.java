import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.math.BigDecimal;

@JsonPropertyOrder({"city", "lat", "lng", "country", "iso2", "adminName", "capital", "population"})
public class CityPojo {
    public String city;               // City name
    public BigDecimal lat;            // Latitude
    public BigDecimal lng;            // Longitude
    public String country;            // Country name
    public String iso2;               // ISO country code
    public String adminName;          // Administrative region
    public String capital;            // Capital or not
    public long population;           // Population
}


