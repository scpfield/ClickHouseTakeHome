/***********************************************************/
/*  clickhouse.cpp                                         */
/***********************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <algorithm>
#include <vector>

/*                                                        */
/*  This is a program that loads text files with data     */
/*  in the format:                                        */
/*                                                        */
/*  <URL><whitespace><Long>                               */
/*                                                        */
/*  Then it sorts by the Long values and prints           */
/*  a Top-N list of the lines.                            */
/*                                                        */
/*  I wrote the code mosty plain C but I leveraged        */
/*  a couple of standard C++ classes for                  */
/*  vector and sort.                                      */
/*                                                        */
/*  To compile, simply run the following g++ command on   */
/*  any linux system that has the default development     */
/*  packages installed:                                   */
/*                                                        */
/*  g++ clickhouse.cpp -o clickhouse                      */
/*                                                        */
/*  There are no special dependencies except perhaps      */
/*  for using the GNU 'getline' function, which should    */
/*  be installed by default on most/all linux systems.    */
/*                                                        */
/*  Upon encountering invalid data, bad formats, etc.,    */
/*  the program intentionally exits early.  If it were    */
/*  intended to be deployed as a production app, more     */
/*  robust recovery would be needed.                      */
/*                                                        */


/*  Globals for the user options */
/*  configured via command-line  */

char*   InputFileName           = NULL;
long    TopDisplayCount         = 10;
char    TopDisplaySortOrder     = 0;      // 0 = Desc, 1 = Asc
bool    GenerateTestDataFile    = false;
char*   OutputFileName          = NULL;
long    NumLinesToGenerate      = 0; 
bool    Verbose                 = false;

/*  Basic struct to use for the input data  */

typedef struct  _DATA_ITEM
{
    char*  URL;
    long   LongValue;

}   DATA_ITEM;


/*  Function declarations  */

bool  CompareFunctionAscending(  DATA_ITEM* Item1,
                                 DATA_ITEM* Item2 );

bool  CompareFunctionDescending(  DATA_ITEM* Item1,
                                  DATA_ITEM* Item2 );

bool  DumpVectorData( std::vector<DATA_ITEM*> *DataVector,
                      long TopCount );

bool  GenerateTestData( const char* Filename, long NumLines );

long  GetCurrentTimeMs();

bool  ParseArgs( int argc, char *argv[] );

void  PrintHelp();


/*  The main function has the bulk of the functionality  */
/*  for loading the input data files, sorting them, and 
/*  displaying the Top N values */

int main( int argc, char *argv[] )
{
    if ( !ParseArgs( argc, argv )) {
          PrintHelp();
          return (1);
    }

    std::vector < DATA_ITEM* >  DataVector;
    DATA_ITEM*  NewItem         = NULL;
    FILE*       DataFile        = NULL;
    char*       InputLine       = NULL;
    char*       TokenLine       = NULL;
    char*       URL             = NULL;
    size_t      Length          = 0;
    long        LongValue       = 0;
    size_t      BufferSize      = 0;
    ssize_t     BytesRead       = 0;
    char*       Token           = NULL;
    char        Delims[]        = { ' ', '\n', '\0' };   
    short       Position        = 0;
    bool        Status          = false;
    bool        HaveURL         = false;
    bool        HaveValue       = false;
    long        Before          = 0;
    long        After           = 0;

    printf( "\n\nThis operating system uses %ld "
            "bytes for a 'long' datatype\n\n", 
            sizeof(long));

    if ( GenerateTestDataFile )  GenerateTestData(
                                    OutputFileName, 
                                    NumLinesToGenerate );


    if ( !InputFileName ) {
        
        printf("\nIf you want to load an input file, "
               "please specify: -i <Filename> \n\n");
        return (1);
    }

    DataFile = fopen( InputFileName, "r" );
    if ( !DataFile ){
        
        printf("Failed to open input file: %s\n", 
                InputFileName );
                
        goto Failed;
    }
    
    Before  =  GetCurrentTimeMs();
    printf( "Loading data from input file: %s\n", InputFileName );

    /*  Using the GNU getline extension, so this probably */
    /*  won't compile on Windows    */

    while (( BytesRead = getline(  &InputLine, 
                                   &BufferSize, 
                                   DataFile )) != -1 )
    {
        URL         = NULL;
        LongValue   = 0;
        TokenLine   = NULL; 
        TokenLine   = strdup( InputLine );
        Position    = 0;
        HaveURL     = false;
        HaveValue   = false;

        /* Tokenize the lines from the input file        */
        /* We are making the assumption that the first   */
        /* column of data is a URL string, and the 2nd   */
        /* column is a long integer type, separated by   */
        /* whitespace */

        for (  Token    =  strtok( TokenLine, Delims );
               Token   !=  NULL;
               Token    =  strtok( NULL, Delims ))
        {               
            Position  +=  1;
            switch ( Position )
            {
                case 1:
               
                    /* First position should be the URL.         */
                    /* We are only doing a very basic check for  */
                    /* whether it really is a URL string.        */

                    if ( strcasestr( Token, "http" )) { 

                        /* Allocate memory from the heap   */
                        /* to store the URL string         */

                        Length = strlen( Token );

                        URL = ( char* ) malloc( 
                                        sizeof ( char ) * 
                                        ( Length + 1 ));

                        if ( !URL ) {

                            printf("Failed to allocate URL\n");
                            goto Failed;
                        }

                        strcpy( URL, Token );
                        HaveURL = true;

                    } else {

                        printf("Token string is not a URL\n");
                        goto Failed;
                    }
                    
                    break;
                    
                case 2:
                           
                    /*  Second position should be the long value        */
                    /*  Just using the regular stdlib number conversion */
                    /*  functions.  First check if we need to handle    */
                    /*  the "0" special case.                           */

                    if  (( strlen( Token )  ==  1 )   && 
                         ( Token[0]  == '0' )){

                        LongValue = 0;

                } else {

                    /* Convert from string to long */
                    LongValue = strtol( Token, NULL, 10 );

                    /*  It potentially sets any error conditions */
                    /*  to one of these values     */

                    if  (( LongValue == LONG_MIN )    &&
                         ( LongValue == LONG_MAX )    &&
                         ( LongValue == 0        ))   {
    
                        printf( "Failed to convert token "
                                "to long value: %s\n", Token);

                        goto Failed;
                    }
                }

                HaveValue = true;
                break;   
               
            default:

              // Nothing to do here, only if there is unexpected 
              // extra data will this get executed. Don't fail,
              // just make a note of it
                
                printf("File has more than 3 columns of data: %s\n", Token);
                break; 

                   
            }   /* End switch */
            
            /* Free the memory that 'getline' and 'strdup' allocated */

            if ( InputLine )
                free( InputLine );

            if ( TokenLine )
                free ( TokenLine );
                
            InputLine = NULL;
            TokenLine = NULL;
            BufferSize = 0;

        }  // End For Loop

        /*  If we have all the data, allocate a new struct  */
        /*  from the heap to put the data in, then add the  */
        /*  struct to a C++ vector, to make it convenient   */
        /*  later on to sort        */

        if  (( HaveURL ) && ( HaveValue )) {
            
            DATA_ITEM*  NewDataItem  = ( DATA_ITEM* )
                        malloc( sizeof( DATA_ITEM   ));

            if  ( !NewDataItem ) {
                    printf("Failed to allocate DATA_ITEM\n");
                    goto Failed;
            }
        
            NewDataItem->URL        = URL;
            NewDataItem->LongValue  = LongValue;
            DataVector.push_back    ( NewDataItem );

        } else {
            printf("Failed to process input data\n");
            goto Failed;
        }

          
    }  /* End Reading Input File */

    After = GetCurrentTimeMs();
    printf("Loaded %ld items in %ldms from file: %s\n",
            DataVector.size(), 
            (After-Before), 
            InputFileName );  

    
        /*  Use the default sorting mechanism in C++ but we   */
        /*  are using a simple custom Comparator function     */
        /*  because we want to sort our data in the C structs */ 
        /*  by the LongValue field.                           */
        /*  In some initial trials on my home PC, the sorting */
        /*  is pretty slow.  Something to look into someday.  */
    
    printf("Sorting data...\n");
    Before = GetCurrentTimeMs();

        /* We could be more efficient by using a typedef      */
        /* function pointer declaration and not need an 'if'  */ 
        /* but no big deal   */

    if  (   TopDisplaySortOrder == 0 ) {       // Descending
                sort(   
                DataVector.begin(), 
                DataVector.end(), 
                CompareFunctionDescending );
    }
    else if (   TopDisplaySortOrder == 1 ) {    // Ascending
                sort(   
                DataVector.begin(),
                DataVector.end(),
                CompareFunctionAscending );
    }
    
    After = GetCurrentTimeMs();
    printf("Sorted %ld items in %ldms\n", 
            DataVector.size(), 
            ( After-Before ));
        
    
    /*  Print the Top results  */
    printf("Top %ld Items\n", TopDisplayCount );
    DumpVectorData( &DataVector, TopDisplayCount );


    /*  There are some cleanup items to do before exiting */
    goto Success;


    Success:
        Status = true;
        goto Cleanup;

    Failed:

      printf("Error code: %d, text: %s\n", 
              errno, strerror(errno));
              
      Status = false;
      goto Cleanup;


    Cleanup:

      while ( !DataVector.empty() ){
          
        DATA_ITEM* DataItem = DataVector.back();
        
        if ( DataItem ) {
            if ( DataItem->URL )
                free( DataItem->URL );
            free( DataItem );
        }
        
        DataVector.pop_back();
    }

    if ( DataFile )
        fclose( DataFile );
        goto Exit;

    Exit:
      return(Status);

}

/*  I made two comparators, because I didn't       */
/*  want to potentially slow it down by having an 'if' */
/*  decision for every comparison for Asc/Desc because */
/*  there is a user config option to select what type  */

bool CompareFunctionAscending(  DATA_ITEM* Item1,
                                DATA_ITEM* Item2 ) 
{  return (( Item1->LongValue ) < ( Item2->LongValue )); }

bool CompareFunctionDescending( DATA_ITEM* Item1,
                                DATA_ITEM* Item2 )
{ return (( Item1->LongValue ) > ( Item2->LongValue )); }


/* Function to dump the data from the C++ vector, either  */
/* for the Top 10, or the whole thing.   . */

bool DumpVectorData( std::vector<DATA_ITEM*> *DataVector,
                     long TopCount  )
{
    if ( TopCount < 1 )
        TopCount = DataVector->size();

    if ( TopCount > DataVector->size() )
    {
        printf("Requested dump size exceeds total size.\n"
               "Vector size = %ld, Requested size = %ld\n"
               "Dumping %ld items\n", DataVector->size(),
               TopCount, DataVector->size()); 
        
        TopCount = DataVector->size();
    }
    
    long PreviousValue = 
        ((DATA_ITEM*) DataVector->at(0))->LongValue;

    for (   long     Index  =  0;
            Index <  TopCount;
            Index += 1 ){

        DATA_ITEM* Item = DataVector->at( Index );

        if ( Verbose )
            printf( "[%ld]  URL = %s, LongValue = %ld  (Delta = %ld)\n",
                Index, 
                ( Item->URL   ), 
                ( Item->LongValue ),
                ( Item->LongValue ) - PreviousValue);
        else
            printf( "%s\n", Item->URL);
            
        
        PreviousValue = ( Item->LongValue );

    }

    return ( true );
}


/*  This function will generate test data files with random      */
/*  numbers in the URL strings and the Long values               */
/*  Turns out the basic stdlib RAND_MAX_SIZE is only a 32-bit    */
/*  value, but we want them to be Longs which, on my computer    */
/*  are 64-bit values.  So I tried a workaround by calling       */
/*  'rand()' twice, and taking the two 32-bit values and         */
/*  combining them into a 64-bit number by shifting one of them  */
/*  into the upper-half of a 64-bit variable, then ORing them    */
/*  together.  It works to make bigger numbers but I make no     */
/*  claims about how good the randomness is.                     */
/*                                                               */
/*  Two of the frankenstein random 64-bit numbers are generared, */
/*  one fo the number in the URL string, and the other for       */
/*  the long column.    */

bool GenerateTestData( const char* Filename, long NumLines )
{
    FILE*   TestDataFile        =   NULL;
    long    Count               =   0;
    bool    Result              =   false;
    int     Status              =   false;
    long    Before              =   0;
    long    After               =   0;

    srand(time(0));

    if ( !Filename ) {
        printf("Please specify an Output Filename "
        "parameter for generating test data\n");
        return(false);
    }
    
    TestDataFile = fopen(Filename, "w+" );

    if ( !TestDataFile ) {
        printf("Failure opening/creating output file\n");
        goto Failed;
    }

    Before = GetCurrentTimeMs();

    for (   Count  =  0; 
            Count  <  NumLines; 
            Count +=  1 ){

        long RandomLong1 =  ((long) rand() << 32) | ((long) rand());
        long RandomLong2 =  ((long) rand() << 32) | ((long) rand());

        int Status       =  fprintf (
                            TestDataFile,
                            "http://api.tech.com/item/%ld %ld\n", 
                            RandomLong1, 
                            RandomLong2 );
    
        if ( Status < 0 ){
             printf("Failed writing to output file\n");
             goto Failed;
        }
    }

    After = GetCurrentTimeMs();
    
    printf("Generated %ld lines of random data in %ld milliseconds to file: %s\n", 
            NumLines, (After-Before), Filename);
    
    goto Success;

    Success:
       Result = true;
       goto Cleanup;

    Failed:
       Result = false;
       goto Cleanup;

    Cleanup:
       if ( TestDataFile )
        fclose( TestDataFile );
       
       goto Exit;

    Exit:
      return ( Result );

}

/* I spent a small bit of time looking at the C++ chronos  */
/* which looks much better than the default stdlib stuff.  */
/* For now just using the traditional time functions.      */

long  GetCurrentTimeMs()
{
    struct timeval  CurrentTime = { 0 };
    gettimeofday( &CurrentTime,  NULL );
    
    time_t  CurrentTimeMs = ( CurrentTime.tv_sec * 1000  ) +
                            ( CurrentTime.tv_usec / 1000 );

     return ( CurrentTimeMs );
}

/*  I know :) There are lots of arg-parser libs   */
/*  out there, no need to re-invent the wheel ... */

bool  ParseArgs( int argc, char* argv[] )
{
    bool Status = false;    
    if ( argc < 2 ) return ( false );

    for ( int arg  =  1;
              arg  <  argc;
              arg  += 1  )
    {
        if ( argv[arg][0] == '-' )
        {
            switch ( argv[arg][1] )
            {
                case 'i':
                
                    if (( arg + 1) < argc )
                        InputFileName = argv[( arg + 1 )];
                    else 
                        goto MissingValue;
                    break;
    
                case 'n':
                
                    if (( arg + 1) < argc )
                        TopDisplayCount = atol( argv[( arg + 1 )] );
                    else  
                        goto MissingValue;      
                    break;

                case 's':
                    
                    if (( arg + 1) < argc )
                        TopDisplaySortOrder = atoi( argv[( arg + 1 )]);
                        if ((TopDisplaySortOrder < 0) ||
                            (TopDisplaySortOrder > 1))
                                goto InvalidValue;
                    else
                        goto MissingValue;
                    break;
            
                case 'o':
                
                    if (( arg + 1) < argc )
                        OutputFileName = argv[( arg + 1 )];
                    else
                        goto MissingValue;
                    break;
            
                case 'v':
                
                    Verbose = true;
                    
                    break;
            
                case 'g':
                    if (( arg + 1) < argc ) {
                        GenerateTestDataFile = true;
                        NumLinesToGenerate = atol( argv[( arg + 1 )] );
                    } else
                        goto MissingValue;
                    break;

                default:
                    break;
            }  // end switch
        } // end if
    } // end for
    
    goto Success;

    Success:
        Status = true;
        goto Cleanup;
    Cleanup:
        goto Exit;
    MissingValue:
        Status = false;
        printf("\n*** Missing value for argument ***\n");
        goto Exit;
    InvalidValue:
        Status = false;
        printf("\n*** Invalid value for argument ***\n");
        goto Exit;
    Exit:
        return ( Status );
}


void PrintHelp()
{
    printf("\n");
    printf("Usage Summary:\n");
    printf("--------------\n\n");
    printf("  -i  <Input File>\n\n");
    printf("      Relative or fully qualified path + filename to the input file.\n");
    printf("      Likely if it contains spaces you will need to enclose in quotes.\n");
    printf("\n");
    printf("  -n  <Top Display Count>\n\n");
    printf("      The default is 10.  Specify a different value if you like. Specifying\n");
    printf("      a negative number will dump everything.\n");
    printf("\n");
    printf("  -s  <Top Display Sort Order>\n\n");
    printf("      Either 0 or 1 for Descending / Ascending.  The default is 0 (Descending).\n");
    printf("\n");
    printf("  -g  <Generate Test Data Lines>\n\n");
    printf("      This will generate a Test Data File with random values.\n");
    printf("      It is a combo-option, '-g 50000' will enable the creation of a test data file\n");
    printf("      with 50,000 lines of URLs and Long numbers.  It is not enabled by default.\n");
    printf("\n");
    printf("  -o  <Test Data Output File>\n\n");
    printf("      The name of the Test Data file if you are generating one.\n");
    printf("\n");
    printf("  -v  <Verbose Output>\n\n");
    printf("      Default is non-verbose to conform to the exact definition of requirments\n");
    printf("      which is only to display the URL strings in the display output\n\n"); 

    return;
}

