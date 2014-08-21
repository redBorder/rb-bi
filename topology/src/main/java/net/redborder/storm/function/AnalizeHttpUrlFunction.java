/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * <p> This enriching analyzes the http url, try looking for facebook users, dropboox users, twitter users, youtube users, google maps locations, linkedin shares, facebook likes and media data (jpg, png, avi, mp3 ...). </p>
 * @author Andres Gomez
 */
public class AnalizeHttpUrlFunction extends BaseFunction {

    /**
     * This variable save the input tuple.
     */
    Map<String, Object> event;

    /**
     * This variable buil the output tuple.
     */
    Map<String, Object> result;

    /**
     * If it is true: debug is ON.
     */
    boolean _debug;

    /**
     * Check if debug is ON or OFF.
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _debug = (boolean) conf.get("rbDebug");
    }

    /**
     * <p> This enriching analyzes the http url, try looking for facebook users, dropboox users, twitter users, youtube users, google maps locations, linkedin shares, facebook likes, facebook shares and media data (jpg, png, avi, mp3 ...). </p>
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        result = new HashMap<>();
        try {
            this.event = event;
            if (event.containsKey("http_host")) {
                String httpHost = event.get("http_host").toString();

                if (_debug)
                    System.out.println("HTTP_HOST: " + httpHost);

                if (httpHost.contains("dropbox.com")) {
                    dropboxUser();
                } else if (httpHost.contains("www.facebook.com")) {
                    facebookLikeShare();
                } else if (httpHost.contains("akamaihd.net")
                        || httpHost.contains("ak.fbcdn.net")) {
                    facebookUser();
                } else if (httpHost.contains("maps.google.com")) {
                    locationGoogleMaps();
                } else if (httpHost.contains("www.linkedin.com")) {
                    linkedinShare();
                } else if (httpHost.contains("api.twitter.com")) {
                    twitterUser1();
                } else if (httpHost.contains("twitter.com")) {
                    twitterUser2();
                } else if (httpHost.contains("gdata.youtube.com")) {
                    youtubeUser();
                } else {
                    mediaData();
                }
            }

            collector.emit(new Values(result));
        }catch (Exception ex){
            result = new HashMap<>();
            collector.emit(new Values(result));
            System.out.println("Could not enrich with Http Analyzer function: " + event.toString() + "\n" + ex);
        }
    }

    /**
     * <p>Search dropbox users and put on "http_social_user".</p>
     */
    private void dropboxUser() {
        String url = event.get("http_url").toString();
        if (url.contains("user_id")) {
            int start = url.indexOf("user_id") + "user_id".length();
            int end1 = url.indexOf("?", start);
            int end2 = url.indexOf("/", start);
            int end3 = url.indexOf("&", start);
            int end = compareLess(end1, compareLess(end2, end3));
            if (_debug)
                System.out.println("Dropbox id: " + url.substring(start + 1, end));

            if (end != 0) {
                result.put("http_social_user", "Dropbox id: " + url.substring(start + 1, end));
            }
        }
    }

    /**
     * <p>Search facebook likes and shares put on "facebook_like" or "facebook_share".</p>
     */
    private void facebookLikeShare() {
        String url = event.get("http_url").toString();
        if (url.contains("plugins/like.php")) {
            int start = url.indexOf("href=") + "href=".length();
            int end = url.indexOf("&", start);

            if (_debug)
                System.out.println("FACEBOOK_LIKE: " + url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));

            result.put("facebook_like",
                    url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));

        } else if (url.contains("/plugins/share_button")) {
            int start = url.indexOf("href=") + "href=".length();
            int end = url.indexOf("&", start);

            if (_debug)
                System.out.println("FACEBOOK_SHARE: " + url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));

            result.put("facebook_share", url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
        }
    }

    /**
     * <p>Search facebook users and put on "http_social_user".</p>
     */
    private void facebookUser() {
        String url = event.get("http_url").toString();
        int start = url.indexOf("_") + 1;
        int end = url.indexOf("_", start);

        if (_debug)
            System.out.println("FACEBOOK_USER: " + "http://www.facebook.com/profile.php?id=" + url.substring(start, end));

        result.put("http_social_user", "http://www.facebook.com/profile.php?id=" + url.substring(start, end));
    }

    /**
     * <p>Search google maps location and put on "google_maps_location".</p>
     */
    private void locationGoogleMaps() {
        String url = event.get("http_url").toString();

        if (url.contains("/maps/api/staticmap?")) {

            int start = url.indexOf("center=") + "center=".length();
            int end = url.indexOf("&", start);
            result.put("google_maps_location",
                    url.substring(start, end).replace(",", "%2C"));
        }
    }

    /**
     * <p>Search linkedin shares and put on "linkedin_share".</p>
     */
    private void linkedinShare() {
        String url = event.get("http_url").toString();
        if (url.contains("share?url=")) {
            int start = url.indexOf("share?url=") + "share?url=".length();
            int end = url.indexOf("&", start);

            if (end < 0) {
                end = url.length();
            }

            if (_debug)
                System.out.println("LINKEDIN_SHARE: " + url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));

            result.put("linkedin_share",
                    url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
        }

    }

    /**
     * <p>Search twitter users and put on "http_social_user".</p>
     */
    private void twitterUser1() {
        String url = event.get("http_url").toString();
        if (url.contains("screen_name")) {
            int start = url.indexOf("screen_name=") + "screen_name=".length();
            int end = url.indexOf("&", start);

            if (_debug)
                System.out.println("TWITTER_USER: " + "http://www.twitter.com/" + url.substring(start, end));
            result.put("http_social_user", "http://www.twitter.com/" + url.substring(start, end));
        }
    }

    /**
     * <p>Search twitter users and put on "http_social_user".</p>
     */
    private void twitterUser2() {
        String url = event.get("http_url").toString();
        if (url.contains("status")) {
            int end = url.indexOf("/");

            if (_debug)
                System.out.println("TWITTER_USER: " + "http://www.twitter.com/" + url.substring(0, end));

            result.put("http_social_user", "http://www.twitter.com/" + url.substring(0, end));
        }
    }

    /**
     * <p>Search youtube users and put on "http_social_user".</p>
     */
    private void youtubeUser() {
        String url = event.get("http_url").toString();
        if (url.contains("/feeds/api/users/")) {
            int start = url.indexOf("/feeds/api/users/") + "/feeds/api/users/".length();
            int end1 = url.indexOf("?", start);
            int end2 = url.indexOf("/", start);
            int end = compareLess(end1, end2);

            if (end != 0) {
                if(_debug)
                    System.out.println("YOUTUBE_USER: " + "https://www.youtube.com/user/" + url.substring(start, end));

                result.put("http_social_user",
                        "https://www.youtube.com/user/" + url.substring(start, end));
            }
        }
    }

    /**
     * <p>Search media data (mpg, jpg, avi, mp3 ...) and put on "http_social_media".</p>
     */
    private void mediaData() {
        String host = event.get("http_host").toString();
        String url = event.get("http_url").toString();
        String extension = url.substring(url.length() - 5, url.length());

        if (extension.contains(".jpg") || extension.contains(".gif")
                || extension.contains(".png") || extension.contains(".mp4")
                || extension.contains(".avi")
                || extension.contains(".mp3")) {

            if(_debug)
                System.out.println("MEDIA_DATA: " + "http://" + host + url);

            result.put("http_social_media",
                    "http://" + host + url);
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
