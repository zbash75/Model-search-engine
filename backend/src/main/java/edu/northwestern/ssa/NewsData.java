package edu.northwestern.ssa;

public class NewsData{
    private String url;
    private String html;
    private String text;
    private String title;

    public NewsData(String url){
        this.url = url;
    }

    public void setHtml(String html){
        this.html = html;
    }

    public void setText(String text){
        this.text = text;
    }

    public void setTitle(String title){
        this.title = title;
    }

    public String getHtml(){
        return this.html;
    }

    public String getUrl(){
        return this.url;
    }

    public String getText(){
        return this.text;
    }

    public String getTitle(){
        return this.title;
    }
}
