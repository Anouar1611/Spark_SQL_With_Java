package Model;

public class Cars {

    private String car;

    private double MPG;

    private int Cylinders;

    private double Displacement;

    private double Horsepower;

    private double Weight;

    private double Acceleration;

    private double Model;

    private String Origin;

    public Cars(String car, double MPG, int cylinders, double displacement, double horsepower, double weight, double acceleration, double model, String origin) {
        this.car = car;
        this.MPG = MPG;
        this.Cylinders = cylinders;
        this.Displacement = displacement;
        this.Horsepower = horsepower;
        this.Weight = weight;
        this.Acceleration = acceleration;
        this.Model = model;
        this.Origin = origin;
    }

    public String getCar() {
        return car;
    }

    public void setCar(String car) {
        this.car = car;
    }

    public double getMPG() {
        return MPG;
    }

    public void setMPG(double MPG) {
        this.MPG = MPG;
    }

    public double getCylinders() {
        return Cylinders;
    }

    public void setCylinders(int cylinders) {
        Cylinders = cylinders;
    }

    public double getDisplacement() {
        return Displacement;
    }

    public void setDisplacement(double displacement) {
        Displacement = displacement;
    }

    public double getHorsepower() {
        return Horsepower;
    }

    public void setHorsepower(double horsepower) {
        Horsepower = horsepower;
    }

    public double getWeight() {
        return Weight;
    }

    public void setWeight(double weight) {
        Weight = weight;
    }

    public double getAcceleration() {
        return Acceleration;
    }

    public void setAcceleration(double acceleration) {
        Acceleration = acceleration;
    }

    public double getModel() {
        return Model;
    }

    public void setModel(double model) {
        Model = model;
    }

    public String getOrigin() {
        return Origin;
    }

    public void setOrigin(String origin) {
        Origin = origin;
    }


    public Cars(){

    }

    @Override
    public String toString() {
        return "Car{" +
                ", car='" + car + '\'' +
                ", MPG='" + MPG + '\'' +
                ", Cylinders='" + Cylinders + '\'' +
                ", Displacement='" + Displacement + '\'' +
                ", Horsepower='" + Horsepower + '\'' +
                ", Weight='" + Weight + '\'' +
                ", Acceleration='" + Acceleration + '\'' +
                ", Model='" + Model + '\'' +
                ", Origin='" + Origin + '\'' +
                '}';
    }
}
