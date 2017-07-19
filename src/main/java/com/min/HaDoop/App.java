package com.min.HaDoop;

/**
 * Hello world!
 *
 */
public class App {
	public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
       // String iString ="123,good";
        //String stringss = null;
        String iString ="675ac22a-25a3-4408-b090-0779759377cc	41580548441010511A5161	T45.001	04	e602fbf4-bb1d-41bb-929c-a688ee18015b	1	2009-07-16	2016-05-02	22885.00	1";
        for (String string : iString.trim().split("")) {
			System.out.println("@:"+string);
		}
    }
}
