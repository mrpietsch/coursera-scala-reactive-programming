package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8

    val prevalenceRate = 0.01
    val transmissibilityRate = 0.4

    val incubationTime = 6
    val mortabilityTime = 14
    val mortabilityRate = 0.25
    val timeToGetImmune = 16
    val timeToGetHealthy = 18
  }

  import SimConfig._

  val persons: List[Person] = (for (i <- 0 to population - 1) yield new Person(i)).toList

  val neighborsDeltas = List((1, 0), (0, 1), (-1, 0), (0, -1))

  // initially infect certain persons
  val initiallyInfected = (prevalenceRate * population).round.toInt
  for (i <- 0 to initiallyInfected - 1) {
    val randomIndex = randomBelow(300)
    persons(randomIndex).infect()
  }

  // initially start moving
  persons.foreach {
    p => p.triggerMove()
  }

  class Person(val id: Int) {

    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    def triggerMove() {
      afterDelay(randomBelow(5) + 1)(move())
    }

    def beHealthy() = {
      infected = false
      sick = false
      immune = false
    }

    def beImmuneAndGetHealthySoon() = {
      sick = false
      immune = true
      infected = true
      afterDelay(timeToGetHealthy - timeToGetImmune)(beHealthy())
    }

    def beDead() = {
      dead = true
      immune = false
      sick = true
      infected = true
    }

    def dieProbablyOrGetImmuneSoon() = {
      if (random <= mortabilityRate) {
        beDead()
      } else {
        afterDelay(timeToGetHealthy - mortabilityTime)(beImmuneAndGetHealthySoon())
      }
    }

    def beSick() = {
      sick = true
      afterDelay(mortabilityTime)(dieProbablyOrGetImmuneSoon())
    }

    def infect() {
      dead = false
      sick = false
      immune = false
      infected = true
      afterDelay(incubationTime)(beSick())
    }

    def infectProbably() {
      if (!infected && !immune) {
        // find infected persons in the room
        val infectedPersons = persons.filter(p => id != p.id && row == p.row && col == p.col && p.infected)
        if (!infectedPersons.isEmpty) {
          if (random <= transmissibilityRate) infect()
        }
      }
    }

    def calculateNeighbors() = {
      neighborsDeltas.map {
        case (rowDelta, colDelta) => ((row + rowDelta + roomRows) % roomRows, (col + colDelta + roomColumns) % roomColumns)
      }
    }

    def personsInRoom(r: Int, c: Int) = {
      persons.filter(p => id != p.id && r == p.row && c == p.col)
    }

    def move() {


      if (!dead) {
        val noOneIsSickPredicate: ((Int, Int)) => Boolean = {
          case (r, c) => personsInRoom(r, c).filter(_.sick).isEmpty
        }

        // look around and filter rooms with obviously sick / dead persons
        val walkableRooms: List[(Int, Int)] = for (
          room <- calculateNeighbors() if noOneIsSickPredicate(room._1, room._2)
        ) yield room

        if (!walkableRooms.isEmpty) {
          val randomIndex: Int = randomBelow(walkableRooms.size)
          val randomNeighbor: (Int, Int) = walkableRooms(randomIndex)

          // go to that room
          row = randomNeighbor._1
          col = randomNeighbor._2

          infectProbably()
        }
        triggerMove()
      }

    }


  }

}
