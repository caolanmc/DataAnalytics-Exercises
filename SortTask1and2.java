import java.util.Arrays;
import java.util.Comparator;

public class SortTask1and2<T> // Class Sort is a generic class with type parameter T
{
    T[] array; // The array of objects of type T we want to sort
    Comparator<T> comp; // A Comparator instance suitable for comparing objects of type T
    public static void main(String[] args)
    {
        //Moved the types of separators into their own methods for cleanliness.
        //These are just called from the main method as seen below.

        System.out.println("\r\nQuestion 1:");
        //Q1
        stringSort();
        doubleSort();

        System.out.println("\r\nQuestion 2:");
        //Q2
        lambdaStringSort();
        lambdaDoubleSort();
    }

    public static void stringSort()
    {
        // A comparator for objects of type String:
        Comparator<String> compString = new Comparator<String>()
        {
            public int compare(String a, String b)
            {
                if (a.compareTo(b) > 0)
                return 1;
                else
                return 0;
            }
        };
        
        SortTask1and2<String> sortStrings = new SortTask1and2<String>();
        
        // Initialising an array of Strings with 16 unordered elements.
        // Array length must be a power of 2.
        String[] arrayOfStrings = { "Blue", "Yellow", "Almond", "Onyx", "Peach", "Gold",
        "Red", "Melon", "Lava", "Beige", "Aqua", "Lilac", "Capri", "Orange", "Mauve", "Plum" };
        
        System.out.println("\r\nOriginal string array: " + Arrays.toString(arrayOfStrings));
        
        // Sorting the array by calling the sort-method
        sortStrings.sort(arrayOfStrings, compString);
        
        System.out.println("Sorted string array: " + Arrays.toString(arrayOfStrings));
    }

    //Q1
    public static void lambdaStringSort()
    {    
        // A comparator for objects of type String:

        //Lambda functions allow us to scrap the need for our a declaration of compare and feed our String a and b directly
        //along into our compareTo. This will be the same method of lambda implementation for the Double comparator.
        Comparator<String> compString = (String a, String b) ->
        {
            if (a.compareTo(b) > 0)
                return 1;
            else
                return 0;
        };
        
        SortTask1and2<String> sortStrings = new SortTask1and2<String>();
        
        // Initialising an array of Strings with 16 unordered elements.
        // Array length must be a power of 2.
        String[] arrayOfStrings = { "Blue", "Yellow", "Almond", "Onyx", "Peach", "Gold",
        "Red", "Melon", "Lava", "Beige", "Aqua", "Lilac", "Capri", "Orange", "Mauve", "Plum" };
        
        System.out.println("\r\nOriginal string array: " + Arrays.toString(arrayOfStrings));
        
        // Sorting the array by calling the sort-method
        sortStrings.sort(arrayOfStrings, compString);
        
        System.out.println("Sorted string array: " + Arrays.toString(arrayOfStrings));
    }

    //Q1
    public static void doubleSort()
    {
        //All that has to be changed for this to work smoothly are the expected data types,
        //Specifically we are just moving from data type String -> Double.

        // A comparator for objects of type Double:
        Comparator<Double> compDouble = new Comparator<Double>()
        {
            public int compare(Double a, Double b)
            {
                if (a.compareTo(b) > 0)
                return 1;
                else
                return 0;
            }
        };
        
        SortTask1and2<Double> sortDouble = new SortTask1and2<Double>();
        
        // Initialising an array of Doubles with 16 unordered elements.
        // Array length must be a power of 2.
        // As we are feeding values to Double[] rather than double[] we need to specify that
        // the given values are in fact double, or they'll be assumed as ints. (No Duck typing!)
        // To do this we simple add "d" to each given element.
        Double[] arrayOfDoubles = {99999d,1d,35d,785d,345d,33d,9546d,8d,79d,10d,1998d,16d,1052d,53252d,63d,101d};
        
        System.out.println("\r\nOriginal double array: " + Arrays.toString(arrayOfDoubles));
        
        // Sorting the array by calling the sort-method
        sortDouble.sort(arrayOfDoubles, compDouble);
        
        System.out.println("Sorted double array: " + Arrays.toString(arrayOfDoubles));
    }

    //Q2
    public static void lambdaDoubleSort()
    {
        // A comparator for objects of type String:
        Comparator<Double> compDouble = (Double a, Double b) ->
        {
            if (a.compareTo(b) > 0)
                return 1;
            else
                return 0;
        };
        
        SortTask1and2<Double> sortDouble = new SortTask1and2<Double>();
        
        // Initialising an array of Strings with 16 unordered elements.
        // Array length must be a power of 2.
        Double[] arrayOfDoubles = {99999d,1d,35d,785d,345d,33d,9546d,8d,79d,10d,1998d,16d,1052d,53252d,63d,101d};
        
        System.out.println("\r\nOriginal double array: " + Arrays.toString(arrayOfDoubles));
        
        // Sorting the array by calling the sort-method
        sortDouble.sort(arrayOfDoubles, compDouble);
        
        System.out.println("Sorted double array: " + Arrays.toString(arrayOfDoubles));
    }
    
    public void sort(T[] array, Comparator<T> comp)
    { // Array length must be a power of 2
        this.array = array;
        this.comp = comp;
        sort(0, array.length);
    }

    private void sort(int low, int n)
    {
        if (n > 1)
        {
            int mid = n >> 1;
            sort(low, mid);
            sort(low + mid, mid);
            combine(low, n, 1);
        }
    }

    private void combine(int low, int n, int st)
    {
        int m = st << 1;
        if (m < n)
        {
            combine(low, n, m);
            combine(low + st, n, m);
            for (int i = low + st; i + st < low + n; i += m)
            compareAndSwap(i, i + st);
        } else
            compareAndSwap(low, low + st);
        }

        private void compareAndSwap(int i, int j)
        {
            if (comp.compare(array[i], array[j]) > 0)
            swap(i, j);
        }

        private void swap(int i, int j)
        {
            T h = array[i];
            array[i] = array[j];
            array[j] = h;
        }
}