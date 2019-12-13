package com.cy.gmall0715.mock.util;

import java.util.Random;

/**
 * @author cy
 * @create 2019-12-13 11:15
 */
public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

