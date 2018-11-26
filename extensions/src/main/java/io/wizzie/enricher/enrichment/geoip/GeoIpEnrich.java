package io.wizzie.enricher.enrichment.geoip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import io.wizzie.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.enricher.enrichment.utils.Constants;
import io.wizzie.metrics.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class GeoIpEnrich extends BaseEnrich {
    private static final Logger log = LoggerFactory.getLogger(GeoIpEnrich.class);

    private String[] baseDimensions = {
            "country_code",
            "country_is_in_european_union",
            "country_name",
            "country_iso_code",
            "continent_name",
            "continent_code",
            "city",
            "longitude",
            "latitude",
            "timezone",
            "as_name",
            "is_anonymous",
            "is_anonymous_vpn",
            "is_hosting_provider",
            "is_tor_exit_node",
            "is_public_proxy",
            "is_legitimate_proxy",
            "timezone",
            "postal_code"
    };

    final static String SRC_DIM = "src.dim";
    final static String DST_DIM = "dst.dim";

    String SRC_IP = "";
    String DST_IP = "";

    Map<String, String> srcDimensionsMap = new HashMap<>();
    Map<String, String> dstDimensionsMap = new HashMap<>();

    /**
     * Pattern to to make the comparison with ips v4.
     */
    private Pattern VALID_IPV4_PATTERN = null;

    /**
     * Pattern to to make the comparison with ips v6.
     */
    private Pattern VALID_IPV6_PATTERN = null;

    /**
     * Pattern to to make the comparison with compressed ips v6.
     */
    private Pattern VALID_COMPRESSED_IPV6_PATTERN = null;

    /**
     * Regular expresion to make the comparison with ipv4 format.
     */
    private static final String ipv4Pattern = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    /**
     * Regular expresion to make the comparison with ipv6 format.
     */
    private static final String ipv6Pattern = "^([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}$";

    /**
     * Regular expresion to make the comparison with compressed ipv6 format.
     */
    private static final String ipv6CompressedPattern = "^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$";

    /**
     * Flag to add additional information.
     */
    private boolean isVerboseMode;

    /**
     * Reference on memory cache to cityReader data base.
     */
    DatabaseReader cityReader;

    /**
     * Reference on memory cache to asnReader data base.
     */
    DatabaseReader asnReader;

    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        // SRC
        SRC_IP = (String) properties.getOrDefault(SRC_DIM, "src");

        for (String dimension : baseDimensions) {
            srcDimensionsMap.put(dimension, (String) properties.getOrDefault(String.format("src.%s.dim", dimension.replace("_", ".")), String.format("src_%s", dimension)));
        }

        // DST
        DST_IP = (String) properties.getOrDefault(DST_DIM, "dst");

        for (String dimension : baseDimensions) {
            dstDimensionsMap.put(dimension, (String) properties.getOrDefault(String.format("dst.%s.dim", dimension.replace("_", ".")), String.format("dst_%s", dimension)));
        }

        // Get database paths
        String ASN_DB_PATH = (String) properties.getOrDefault(Constants.ASN_DB_PATH, "/opt/share/GeoIP2/GeoLite2-ASN.mmdb");
        String CITY_DB_PATH = (String) properties.getOrDefault(Constants.CITY_DB_PATH, "/opt/share/GeoIP2/GeoLite2-City.mmdb");

        // Get if verbose mode is enable
        isVerboseMode = (boolean) properties.getOrDefault(Constants.ENABLE_VERBOSE_MODE, false);

        try {
            // Create city and asn readers
            cityReader = new DatabaseReader.Builder(new File(CITY_DB_PATH)).build();
            asnReader = new DatabaseReader.Builder(new File(ASN_DB_PATH)).build();
            // Create IPv4 and IPv6 validation patterns
            VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern, Pattern.CASE_INSENSITIVE);
            VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern, Pattern.CASE_INSENSITIVE);
            VALID_COMPRESSED_IPV6_PATTERN = Pattern.compile(ipv6CompressedPattern, Pattern.CASE_INSENSITIVE);
        } catch (IOException ex) {
            log.error(ex.toString(), ex);
        } catch (PatternSyntaxException e) {
            log.error("Unable to compile IP check patterns");
        }
    }

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> geoIPMap = new HashMap<>(message);

        // Get stc and dst IP
        String srcIPAddress = (String) message.get(SRC_IP);
        String dstIPAddress = (String) message.get(DST_IP);

        Map<String, Object> srcData = new HashMap<>();

        if (VALID_IPV4_PATTERN.matcher(srcIPAddress).matches() || VALID_IPV6_PATTERN.matcher(srcIPAddress).matches() || VALID_COMPRESSED_IPV6_PATTERN.matcher(srcIPAddress).matches()) {
            try {
                srcData = getDataByIpAndSite(
                        srcIPAddress,
                        dstDimensionsMap);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (GeoIp2Exception e) {
                e.printStackTrace();
            }
        }

        Map<String, Object> dstData = new HashMap<>();

        if (VALID_IPV4_PATTERN.matcher(dstIPAddress).matches() || VALID_IPV6_PATTERN.matcher(dstIPAddress).matches() || VALID_COMPRESSED_IPV6_PATTERN.matcher(dstIPAddress).matches()) {
            try {
                dstData = getDataByIpAndSite(
                        dstIPAddress,
                        srcDimensionsMap);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (GeoIp2Exception e) {
                e.printStackTrace();
            }
        }

        if(srcData != null) geoIPMap.putAll(srcData);
        if(dstData != null) geoIPMap.putAll(dstData);

        return geoIPMap;
    }

    public Map<String, Object> getDataByIpAndSite(String ip, Map<String, String> dimensionMap) throws IOException, GeoIp2Exception {
        Map<String, Object> geoIP2Map = null;

        if (ip != null) {
            geoIP2Map = new HashMap<>();
            Map<String, Object> dataToEnrich = getIpDataInformation(ip);

            for (Map.Entry<String, String> dimensionPair : dimensionMap.entrySet()) {
                Object value = dataToEnrich.get(dimensionPair.getKey());
                if (value != null) geoIP2Map.put(dimensionPair.getValue(), value);
            }
        }

        return geoIP2Map;
    }

    public void stop() {
        try {
            asnReader.close();
            cityReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <p>Query if there is a countryReader code for a given IP.</p>
     *
     * @param ip This is the address to query the data base.
     * @return The countryReader code, example: US, ES, FR.
     */
    private Map<String, Object> getIpDataInformation(String ip) throws IOException, GeoIp2Exception {
        Map<String, Object> locations = new HashMap<>();
        CityResponse cityResponse = cityReader.city(InetAddress.getByName(ip));
        AsnResponse asnResponse = asnReader.asn(InetAddress.getByName(ip));

        if (asnResponse != null) {
            locations.put("as_name", asnResponse.getAutonomousSystemOrganization());
        }

        if (cityResponse != null) {
            Country country = cityResponse.getCountry();
            City city = cityResponse.getCity();
            Location location = cityResponse.getLocation();
            Postal postal = cityResponse.getPostal();
            Traits traits = cityResponse.getTraits();

            // Country
            locations.put("country_code", country.getIsoCode());
            locations.put("is_in_european_union", country.isInEuropeanUnion());

            // City
            locations.put("city_name", city.getName());

            // Location
            locations.put("latitude", location.getLatitude());
            locations.put("longitude", location.getLongitude());

            if (isVerboseMode) {
                locations.put("country_iso_code", country.getIsoCode());
                locations.put("country_name", country.getName());

                // Continent
                Continent continent = cityResponse.getContinent();
                locations.put("continent_name", continent.getName());
                locations.put("continent_code", continent.getCode());

                locations.put("is_anonymous", traits.isAnonymous());
                locations.put("is_anonymous_vpn", traits.isAnonymousVpn());
                locations.put("is_hosting_provider", traits.isHostingProvider());
                locations.put("is_legitimate_proxy", traits.isLegitimateProxy());
                locations.put("is_public_proxy", traits.isPublicProxy());
                locations.put("is_tor_exit_node", traits.isTorExitNode());

                locations.put("timezone", location.getTimeZone());

                locations.put("postal_code", postal.getCode());
            }
        }

        return locations;
    }

}
