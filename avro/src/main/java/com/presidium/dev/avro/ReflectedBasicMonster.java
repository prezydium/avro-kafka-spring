package com.presidium.dev.avro;

public class ReflectedBasicMonster {

    private String name;
    private float attack;
    private int healthPoints;
    private int armor;
    private int loot;

    public ReflectedBasicMonster() {
    }

    public ReflectedBasicMonster(String name, float attack, int healthPoints, int armor, int loot) {
        this.name = name;
        this.attack = attack;
        this.healthPoints = healthPoints;
        this.armor = armor;
        this.loot = loot;
    }

    public String getName() {
        return name;
    }

    public float getAttack() {
        return attack;
    }

    public int getHealthPoints() {
        return healthPoints;
    }

    public int getArmor() {
        return armor;
    }

    public int getLoot() {
        return loot;
    }
}
