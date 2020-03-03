package cn.oyohotels;

public class playPOJO {


    private String id;
    private String name;

    public playPOJO(){

        super();
    }

    @Override
    public String toString() {
        return id + " " + name;
    }

    public playPOJO(String id, String name){
        this.id = id;
        this.name = name;
    }

}
