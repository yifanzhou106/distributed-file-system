package edu.usfca.cs.dfs.StorageNode;

import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.StorageNode.StorageNode.isShutdown;
import java.util.Scanner;


/**
 * UI: handle with user input
 */
public class UI implements Runnable {

    private ExecutorService threads;
    private FileMap fm;


    public UI(ExecutorService threads, FileMap fm) {
        this.threads = threads;
        this.fm = fm;
    }

    @Override
    public void run() {
        while (!isShutdown) {
            Scanner reader = new Scanner(System.in);
            System.out.println("Enter your choices (Enter \"help\" for help): ");
            String userChoice = reader.nextLine();
            String[] splitedUserChoice = userChoice.split(" ");
            String filename;

            switch (splitedUserChoice[0]) {
                case "help":
                    System.out.println("\n**************************");
                    System.out.println("\n1. delete\n");
                    System.out.println("\n**************************");
                    break;

                case "delete":
                    System.out.println("File name? ");
                    filename = reader.nextLine();
                    threads.submit(new DeleteFile(threads,fm, filename));
                    break;

                default:
                    System.out.println("\nWrong Input\n");
                    break;
            }
        }
    }
}




