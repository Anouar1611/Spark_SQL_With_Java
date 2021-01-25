package me.asmai.project.service;
import me.asmai.project.Model.Cars;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
import static me.asmai.project.repository.EntryPoint.*;

public class CarsService {

    public void getAllCars(int numberRows){
        getDataset().show(numberRows);
    }

    public void getCarsByModel(double model) {
        Dataset<Cars> cars = getDataset().filter("Model == \"" + model + "\"");
        cars.show((int) getDataset().count());
    }

    public void getModelOfCarsByLessHorsePower(){
        Dataset<Row> cars = sparkSession().sql("SELECT Model,MIN(Horsepower) FROM cars GROUP BY Model");
        cars.show((int) getDataset().count());
    }

    public void getCarsSortedByModelAndHorsePower(){
        Dataset<Cars> cars = getDataset().sort("Model","Horsepower"); // Or create a Comparator
        cars.show((int) getDataset().count());
    }

    public void getCarsByModelAndOriginAndSortedByHorsePower(double model, String origin){
        Dataset<Cars> cars = getDataset().filter((FilterFunction<Cars>) car -> car.getOrigin().equals(origin))
        .filter("Model == \"" + model + "\"")
        .sort("Horsepower");
        cars.show((int) getDataset().count());
    }

    public void getCarsByOriginAndSortedByModel(String origin){
        Dataset<Cars> cars = getDataset().filter((FilterFunction<Cars>) car -> car.getOrigin().equals(origin))
        .sort("Model");
        cars.show((int) getDataset().count());
    }


}
