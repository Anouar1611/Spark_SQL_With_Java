package me.asmai.project.Main;

import me.asmai.project.service.CarsService;

import java.util.Scanner;


public class Menu {

    public static Scanner scanner = new Scanner(System.in);
    public static CarsService carsService = new CarsService();

    public static void main(String[] args) {

        try {
            int menuOption = 0;
            double model1;
            double model;
            double model2;
            String origin;

            do {
                menuOption = showMenu();

                switch (menuOption) {

                    case 1:
                        System.out.println("Enter the rows number of Cars that you want preview : ");
                        int numberRows = scanner.nextInt();
                        carsService.getAllCars(numberRows);
                        break;
                    case 2:
                        System.out.println("Enter the model of car :  Ex= 70");
                        scanner.nextLine();//// Adding nextLine just to discard the old \n character
                        model = scanner.nextInt();
                        carsService.getCarsByModel(model);
                        break;
                    case 3:
                        carsService.getModelOfCarsByLessHorsePower();
                        break;
                    case 4:
                        carsService.getCarsSortedByModelAndHorsePower();
                        break;
                    case 5:
                        System.out.println("Enter the model of the car : Ex: 80");
                        scanner.nextLine();
                        model1 = scanner.nextInt();
                        System.out.println("Enter the second model of the car : Ex: 81");
                        scanner.nextLine();
                        model2 = scanner.nextInt();
                        System.out.println("Enter the origin of the car : Ex: US");
                        scanner.nextLine();
                        origin = scanner.nextLine();
                        carsService.getCarsBetweenTwoModelsOfAnOriginAndSortedByHorsePower(model1,model2,origin);
                        break;
                    case 6:
                        System.out.println("Enter the origin of the car : Ex: Japan");
                        scanner.nextLine();
                        origin = scanner.nextLine();
                        carsService.getCarsByOriginAndSortedByModel(origin);
                    case 7:
                        System.out.println("Quitting Program...");
                        break;
                    default:
                        System.out.println("Sorry, please enter a valid Option");
                }

            } while (menuOption != 7);

            // Exiting message when user decides to quit Program
            System.out.println("Thanks for using this Program...");

        } catch (Exception ex) {
            System.out.println("Sorry problem occured within Program");
            scanner.next();
        } finally {
            scanner.close();
        }
    }
    public static int showMenu() {
        int option = 0;

        System.out.println("Menu:");
        System.out.println("1. Get All Cars form CSV file ");
        System.out.println("2. Get Cars By Model");
        System.out.println("3. Get Model of Cars By Less HorsePower");
        System.out.println("4. Get Cars Sorted by Model and HorsePower");
        System.out.println("5. Get Cars By Origin and Between Two Models and Sorted by HorsePower");
        System.out.println("6. Get Cars by Origin and Sorted by Model");
        System.out.println("7. Quit Program");

        // Getting query number from above menu
        System.out.println("Enter the number of Query from above...");
        option = scanner.nextInt();

        return option;
    }
}
