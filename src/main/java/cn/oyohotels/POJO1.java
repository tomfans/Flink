package cn.oyohotels;

public class POJO1 {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "POJO1{" +
                "name='" + name + '\'' +
                '}';
    }

    private String name;

}
