

Scala is a programming language that is compatible with Java

## Basics of Scala

- types: `Int`, `Double`, `String`, `Boolean`, `Unit` (empty type)
- you don't need to use "new" (it makes it by default if the class has companion object)
- it has mutable and immutable collections. Immutable collections are great for parellel programming! 
- functions are objects too! 
- don't use `return`! The last evaluated expression is returned. 

```
// define a variable
val a = 10       // this is value (immutable) you don't have to put ; at the end of command
val a:Int = 10   // you can declare a type, but you don't have to
var b = 10       // this is a variable (mutable)
b = 20  // works
a = 20  // doesn't work

// define a function
def square(x:Double) = { x*x }   // the return type will be inferred
def square(x:Int):Int = { x*x }  // declare a function with input type and return type; returns the last evaluated type
def square(x) = { x*x }          // error ! you need to declare the input type
val square: Int=>Int = x => x*x  // function is an object! of type Int => Int

// define a collection
val mylist = 1 to 10   // you don't need . to chain methods if it is inferred. This is actually  1.to(10) - calling the method "to" on Int 1 with argument 10
mylist.toList          // conversion to List 
mylist.toArray         // convertion to Java Array
mylist(4)              // getting an element of the list is calling a function, hence the round brackets

mylist.map(square)     // apply function square to every element of the mylist
mylist.map( x => 2*x)  // apply an anonymous function
mylist.filter( x => x % 2 == 0 )  // filter by a function that maps each element to a Boolean
mylist.foldLeft(100)(_ + _)  // sum all the elements in the list, starting value being 100

// string interpolation 
val DIR = "/path/to/dir"
println( s"this is my $DIR " )

// control statements
if (2==3) 4 else 10   // if-then statement - each branch returns a result
for (a <- mylist) {println(a)} // a for-loop
for( a <- mylist  if a != 3; if a < 8 ){ println( "Value of a: " + a ) }
```


