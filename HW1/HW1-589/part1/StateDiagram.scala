object StateDiagram {	
	def main(args: Array[String]){
		val states = List ("blue sky", "crazy", "jump", "free fall", "parachute", "alive", "dead", "cloudy") 
		var myState = "" 
		println ("I have a friend " ) 
		println ("Sometimes she is " + states(1) ) 
		print  ("When it is ") 

		val r = scala.util.Random 
		var chance = r.nextInt(100)      

		if (chance >=50) {        
			myState = states(7)        
			println(myState)        
			println ("She is " + states(5) ) 
		} 
		else {        
			myState = states(0)        
			println(myState)        
			println("She " + states(2))
			
			val r_1 = scala.util.Random
			var chance_1 = r_1.nextInt(100) 

			if (chance_1 <30) {
				println ("She has a " + states(4) )
				println ("She is " + states(5) )
			} 
			else {
				println ("She experiences " + states(3) )

				val r_2 = scala.util.Random
				var chance_2 = r_2.nextInt(100)

				if (chance_2 <80) {
					println ("She has a " + states(4) )
					println ("She is " + states(5) )
				}
				else {
					println ("She is " + states(6) )
				}			
			}
		}
	}
}