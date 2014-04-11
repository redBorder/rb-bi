/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class AnalizeHttpUrlFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        Map<String, Object> result = new HashMap<>();

        if (event.containsValue("http_host")) {
            /* Dropbox User */
            if (event.get("http_host").toString().contains("dropbox.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("user_id")) {
                    int start = url.indexOf("user_id") + "user_id".length();
                    int end1 = url.indexOf("?", start);
                    int end2 = url.indexOf("/", start);
                    int end3 = url.indexOf("&", start);
                    int end = compareLess(end1, compareLess(end2, end3));

                    if (end != 0) {
                        result.put("http_social_user", url.substring(start, end));
                        collector.emit(new Values(result));
                    } else {
                        collector.emit(new Values(result));
                    }
                } else {
                    collector.emit(new Values(result));
                }
                /* Like-Share Facebook */
            } else if (event.get("http_host").toString().contains("www.facebook.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("plugins/like.php")) {
                    int start = url.indexOf("href=") + "href=".length();
                    int end = url.indexOf("&", start);

                    result.put("facebook_like",
                            url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
                    collector.emit(new Values(result));

                } else if (url.contains("/plugins/share_button")) {
                    int start = url.indexOf("href=") + "href=".length();
                    int end = url.indexOf("&", start);
                    result.put("facebook_share", url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
                    collector.emit(new Values(result));
                } else {
                    collector.emit(new Values(result));
                }
                /* User Facebook */
            } else if (event.get("http_host").toString().contains("akamaihd.net")
                    || event.get("http_host").toString().contains("ak.fbcdn.net")) {
                String url = event.get("http_url").toString();
                int start = url.indexOf("_") + 1;
                int end = url.indexOf("_", start);
                result.put("http_social_user", "http://www.facebook.com/profile.php?id=" + url.substring(start, end));
                collector.emit(new Values(result));

                /* Location Google Maps */
            } else if (event.get("http_host").toString().contains("maps.google.com")) {
                String url = event.get("http_url").toString();

                if (url.contains("/maps/api/staticmap?")) {

                    int start = url.indexOf("center=") + "center=".length();
                    int end = url.indexOf("&", start);
                    result.put("google_maps_location",
                            url.substring(start, end).replace(",", "%2C"));

                } else {
                    collector.emit(new Values(result));
                }

                /* LinkedIn Share */
            } else if (event.get("http_host").toString().contains("www.linkedin.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("share?url=")) {
                    int start = url.indexOf("share?url=") + "share?url=".length();
                    int end = url.indexOf("&", start);

                    if (end < 0) {
                        end = url.length();
                    }

                    result.put("linkedin_share",
                            url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
                    collector.emit(new Values(result));

                } else {
                    collector.emit(new Values(result));
                }

                /* Twitter User */
            } else if (event.get("http_host").toString().contains("api.twitter.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("screen_name")) {
                    int start = url.indexOf("screen_name=") + "screen_name=".length();
                    int end = url.indexOf("&", start);
                    result.put("http_social_user", "http://www.twitter.com/" + url.substring(start, end));
                    collector.emit(new Values(result));
                } else {
                    collector.emit(new Values(result));
                }
                /* Twitter User */
            } else if (event.get("http_hosts").toString().contains("twitter.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("status")) {
                    int end = url.indexOf("/");
                    result.put("http_social_user", "http://www.twitter.com/" + url.substring(0, end));
                    collector.emit(new Values(result));
                } else {
                    collector.emit(new Values(result));
                }
                /*Youtube User*/
            } else if (event.get("http_host").toString().contains("gdata.youtube.com")) {
                String url = event.get("http_url").toString();
                if (url.contains("/feeds/api/users/")) {
                    int start = url.indexOf("/feeds/api/users/") + "/feeds/api/users/".length();
                    int end1 = url.indexOf("?", start);
                    int end2 = url.indexOf("/", start);
                    int end = compareLess(end1, end2);

                    if (end != 0) {
                        result.put("http_social_user",
                                "https://www.youtube.com/user/" + url.substring(start, end));
                        collector.emit(new Values(result));
                    } else {
                        collector.emit(new Values(result));
                    }
                } else {
                    collector.emit(new Values(result));
                }

                /* Media Data */
            } else {
                String host = event.get("http_host").toString();
                String url = event.get("http_url").toString();
                String extension = url.substring(url.length() - 5, url.length());

                if (extension.contains(".jpg") || extension.contains(".gif")
                        || extension.contains(".png") || extension.contains(".mp4")
                        || extension.contains(".avi")
                        || extension.contains(".mp3")) {

                    result.put("http_social_media",
                            "http://" + host + url);
                    collector.emit(new Values(result));

                } else {
                    collector.emit(new Values(result));
                }
            }
        } else {
            collector.emit(new Values(result));
        }

    }

    /**
     * Compare that number is lower
     *
     * @param end1 Number 1
     * @param end2 Numer 2
     * @return The lower number.
     */
    private int compareLess(int end1, int end2) {

        int end = 0;
        if (end1 <= 0) {
            end1 = 10000000;
        }
        if (end2 <= 0) {
            end2 = 10000000;
        }

        if (end1 < end2) {
            end = end1;
        } else if (end1 > end2) {
            end = end2;
        }

        return end;
    }

}
