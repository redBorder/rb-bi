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
    
    Map<String, Object> result = new HashMap<>();
    Map<String, Object> event;
    boolean debug;
    
    public AnalizeHttpUrlFunction(boolean debug){
        this.debug=debug;
    }
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        this.event = event;
        
        if (event.containsValue("http_host")) {
            String httpHost = event.get("http_host").toString();
            System.out.println("http_host is: " + httpHost);
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
        result.clear();    
    }
    
    private void dropboxUser() {
        String url = event.get("http_url").toString();
        System.out.println("Dropbox user found. URL is: " + url);
        if (url.contains("user_id")) {
            int start = url.indexOf("user_id") + "user_id".length();
            int end1 = url.indexOf("?", start);
            int end2 = url.indexOf("/", start);
            int end3 = url.indexOf("&", start);
            int end = compareLess(end1, compareLess(end2, end3));
            
            if (end != 0) {
                System.out.println("Dropbox user is: " + url.substring(start, end));
                result.put("http_social_user", url.substring(start, end));
            }
        }
    }
    
    private void facebookLikeShare() {
        String url = event.get("http_url").toString();
        if (url.contains("plugins/like.php")) {
            int start = url.indexOf("href=") + "href=".length();
            int end = url.indexOf("&", start);
            
            result.put("facebook_like",
                    url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
            
        } else if (url.contains("/plugins/share_button")) {
            int start = url.indexOf("href=") + "href=".length();
            int end = url.indexOf("&", start);
            result.put("facebook_share", url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
        }
    }
    
    private void facebookUser() {
        String url = event.get("http_url").toString();
        int start = url.indexOf("_") + 1;
        int end = url.indexOf("_", start);
        result.put("http_social_user", "http://www.facebook.com/profile.php?id=" + url.substring(start, end));
    }
    
    private void locationGoogleMaps() {
        String url = event.get("http_url").toString();
        
        if (url.contains("/maps/api/staticmap?")) {
            
            int start = url.indexOf("center=") + "center=".length();
            int end = url.indexOf("&", start);
            result.put("google_maps_location",
                    url.substring(start, end).replace(",", "%2C"));
        }
    }
    
    private void linkedinShare() {
        String url = event.get("http_url").toString();
        if (url.contains("share?url=")) {
            int start = url.indexOf("share?url=") + "share?url=".length();
            int end = url.indexOf("&", start);
            
            if (end < 0) {
                end = url.length();
            }
            
            result.put("linkedin_share",
                    url.substring(start, end).replaceAll("%3A", ":").replaceAll("%2F", "/"));
        }
        
    }
    
    private void twitterUser1() {
        String url = event.get("http_url").toString();
        if (url.contains("screen_name")) {
            int start = url.indexOf("screen_name=") + "screen_name=".length();
            int end = url.indexOf("&", start);
            result.put("http_social_user", "http://www.twitter.com/" + url.substring(start, end));
        }
    }
    
    private void twitterUser2() {
        String url = event.get("http_url").toString();
        if (url.contains("status")) {
            int end = url.indexOf("/");
            result.put("http_social_user", "http://www.twitter.com/" + url.substring(0, end));
        }
    }
    
    private void youtubeUser() {
        String url = event.get("http_url").toString();
        if (url.contains("/feeds/api/users/")) {
            int start = url.indexOf("/feeds/api/users/") + "/feeds/api/users/".length();
            int end1 = url.indexOf("?", start);
            int end2 = url.indexOf("/", start);
            int end = compareLess(end1, end2);
            
            if (end != 0) {
                result.put("http_social_user",
                        "https://www.youtube.com/user/" + url.substring(start, end));
            }
        }
    }
    
    private void mediaData() {
        String host = event.get("http_host").toString();
        String url = event.get("http_url").toString();
        String extension = url.substring(url.length() - 5, url.length());
        
        if (extension.contains(".jpg") || extension.contains(".gif")
                || extension.contains(".png") || extension.contains(".mp4")
                || extension.contains(".avi")
                || extension.contains(".mp3")) {
            
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
