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

/* -------------------------------------------------- */
/*  To compile:  g++ clickhouse.cpp -o clickhouse
/* -------------------------------------------------- */

/*  Globals for the user options */
/*  configured via command-line  */

#define SELECTION_TYPE_NORMAL   0
#define SELECTION_TYPE_RANDOM   1
#define SORT_TYPE_DESCENDING    0
#define SORT_TYPE_ASCENDING     1

char*   InputFileName           = NULL;
long    BatchSize               = 1000;
char    SelectionType           = SELECTION_TYPE_NORMAL;
long    ResultCount             = 10;     
char    ResultSortType          = SORT_TYPE_DESCENDING;
bool    GenerateTestDataFile    = false;
char*   OutputFileName          = NULL;  // if generating a test data file
long    NumLinesToGenerate      = 0; 
long    BucketCount             = 4;
bool    Verbose                 = false;

/*  Basic struct to use for the input data  */
typedef struct  _DATA_ITEM
{
    char*  URL;
    long   LongValue;
}   DATA_ITEM;

/* Wrapper struct for the R-Algorithm selection   */
/* that preserves the original index from where   */
/* it came from in the reservoir / data-stream,   */
/* so that we can do a post-selection test to     */
/* see how evenly distributed they were.          */
typedef struct _SAMPLE_ITEM
{
    DATA_ITEM* DataItem;
    long SampleIndex;
}   SAMPLE_ITEM;


/* Data struct for the Histogram/Bucket report */
typedef struct _BUCKET
{
    long Count;
    long MaxValue;
} BUCKET;

/* typedef of a sort compare function  */
typedef bool ( *SORT_COMPARE_FUNCTION ) ( DATA_ITEM*, DATA_ITEM* ); 

/*  Function declarations  */

DATA_ITEM*      GetNextDataItem         ( FILE** FilePtr );
bool            GenerateAlgorithmR      ( FILE** FilePtr );
void            PrintHistogramSummary   ( SAMPLE_ITEM** Reservoir, 
                                          long ItemsRead );
bool            CompareAscending        ( DATA_ITEM* Item1,
                                          DATA_ITEM* Item2 );
bool            CompareDescending       ( DATA_ITEM* Item1,
                                          DATA_ITEM* Item2 );
bool            PrintVectorData         ( std::vector<DATA_ITEM*> *DataVector );
bool            GenerateTestData        ( const char* Filename, long NumLines );
bool            ParseArgs               ( int argc, char *argv[] );
long            GetCurrentTimeMs        ();
void            PrintHelp               ();


bool GenerateAlgorithmR( FILE** FilePtr )
{
    /* Initialize a fixed-size array called the Reservoir for the     */
    /* candidate data samples that are selected from a data           */
    /* stream of unknown size.                                        */
    /* The default size of our reservoir is 10, similar to a          */
    /* "Top 10" style of results, but the size is configurable        */
    /* by the user   */
    
    /*  The Reservoir variable is a heap-allocated                  */
    /*  array of pointers to SAMPLE_ITEM structs that we            */
    /*  get from the input file of unknown size.                    */
    /*  SAMPLE_ITEM structs are wrappers that contain               */
    /*  the DATA_ITEM data from the file.                           */
    
    if ( !FilePtr ) return ( false );
    
    size_t          ReservoirSize    = ( ResultCount * 
                                        sizeof( SAMPLE_ITEM* ));
                                        
    SAMPLE_ITEM**    Reservoir       = ( SAMPLE_ITEM** ) 
                                        malloc( ReservoirSize );
                                   
    DATA_ITEM*      DataItem         = NULL;
    bool            Status           = false;
    long            StartSamplingTs  = 0;
    long            EndSamplingTs    = 0;
    long            ReplacedCount    = 0;
    
    /* this is a short-term hack only used for printing results  */
    /* not used in actual reading of the file or processing data */
    std::vector<DATA_ITEM*> TmpVector;
    
    if ( !Reservoir ) return ( false );
    memset( Reservoir, '\0', sizeof( SAMPLE_ITEM** ));
    
    /* First, populate the Reservoir with an initial set    */  
    /* of data samples from the stream.                    */
    long ReservoirIndex = 0;
    long SampleIndex = 0;
    
    printf("Populating Reservoir with %lu items\n", ResultCount);
    
    /*  In this stage, ReservoirIndex == SampleIndex because we are just  */
    /*  filling Reservoir with the first ResultCount items from the file */
    for (   ReservoirIndex = 0; 
            ReservoirIndex < ResultCount;
            ReservoirIndex += 1) {
                    
        /*  Retrieve an item of data from the data stream.  */
        DataItem = GetNextDataItem( FilePtr );
        
        /*  Abort if we get an invalid data item */
        if ( !DataItem ) goto Failed;
        
        /*  Allocate a new SAMPLE_ITEM that wraps a regular DataItem   */
        SAMPLE_ITEM*  SampleItem = ( SAMPLE_ITEM* ) 
                                    malloc( sizeof ( SAMPLE_ITEM ));
        
        if ( !SampleItem ) goto Failed;
        memset( SampleItem, '\0', sizeof( SAMPLE_ITEM ));
        
        /* Fill the struct */
        SampleItem -> DataItem      = DataItem;
        SampleItem -> SampleIndex   = ReservoirIndex;
        
        /* Add it to the Reservoir array */
        Reservoir[ ReservoirIndex ] = SampleItem;
        
        printf("Populated initial Reservoir[%lu] array item\n", ReservoirIndex);
    }
    
    /*  Now, continue to read lines of data from the file stream.              */
    /*  Each one is a candidate to be selected as a sample to copy             */
    /*  to the reservoir array, which we already populated with                */
    /*  initial items.  Any new candidates that are selected as samples        */
    /*  replaces an existing item in the reservoir.                            */
    /*  The SampleIndex number is a counter that increments as we read         */
    /*  new data items from the data stream.                                   */
    ReservoirSize = ReservoirIndex;
    SampleIndex = ReservoirSize - 1;
    srand( time(0) );
    DataItem = NULL;
    StartSamplingTs = GetCurrentTimeMs();
 
    /*  Start reading data */
    printf("\nReading data + selecting samples from input file\n");
    while ( true )
    {
        /*  Get next data item from file stream */
        DataItem = GetNextDataItem( FilePtr );
        
        /*  If we get a NULL DataItem it means end of file (or failure)  */
        if ( !DataItem ) break;  
        
        /* Increment the sample index counter  */
        SampleIndex += 1;
        
        /*  Now, decide whether to select or reject the item.           */
        /*  Generate a random number between:                           */
        /*    1 -> SampleIndex  (which keeps growing unbounded)         */
        /*  If the value falls within the size of the Reservoir array,  */
        /*  (which remains fixed at its original array size),           */
        /*  then keep the item and replace the existing Reservoir       */
        /*  array element with the new item, using the random number    */
        /*  as an array-lookup index into the Reservoir array           */
        
        /*  The std C library rand() only generates 32-bit values       */
        /*  So we call it twice and make a 64-bit number, then          */
        /*  modulo with SampleIndex     */
        long RandomValue = ((((long) rand() << 32) | ((long) rand()))  
                            % (SampleIndex));
        
        /*  If the number falls within the size of the Reservoir  */
        if ( RandomValue <= ReservoirSize-1 )
        {
            /*  Item is selected. */
            /*  Make a new SAMPLE_ITEM struct to replace the existing one. */
            if ( Verbose ) printf("Selected item SampleIndex=%lu "
                                  "to replace Reservoir[%lu]\n",
                                  SampleIndex, RandomValue );
                    
            SAMPLE_ITEM*  SampleItem = ( SAMPLE_ITEM* ) 
                                        malloc( sizeof ( SAMPLE_ITEM ));
            
            if ( !SampleItem ) goto Failed;
            memset( SampleItem, '\0', sizeof( SAMPLE_ITEM ));
            
            /* Fill the struct, keeping a record of the SampleIndex value */
            SampleItem -> DataItem      = DataItem;
            SampleItem -> SampleIndex   = SampleIndex;
            
            /* Remove the existing Reservoir array item and free the memory */
            if  ( Reservoir[RandomValue] )
                if  ( Reservoir[RandomValue] -> DataItem )
                    if  ( Reservoir[RandomValue] -> DataItem -> URL ) 
                        free( Reservoir[RandomValue] -> DataItem -> URL );
                    free( Reservoir[RandomValue] -> DataItem );
                free( Reservoir[RandomValue] );
            
            /*  Replace the existing Reservoir array entry with the new sample  */
            Reservoir[RandomValue] = SampleItem;
            ReplacedCount += 1;
        }
        else
        {
            if (Verbose) printf("Rejected item SampleIndex=%lu "
                                "because RandomValue=%lu > ReservoirSize=%lu\n",
                                SampleIndex, RandomValue, ReservoirSize);
        }
    }

    EndSamplingTs = GetCurrentTimeMs();

    printf("Finished sample selection in %lu ms\n", 
            (EndSamplingTs-StartSamplingTs));

    printf("Data items read from file = %lu \n", 
            SampleIndex+1);

    printf("Reservoir replacements = %lu \n", 
            ReplacedCount);


    /*  Stuffing results into a vector for the moment because */
    /*  my summary function currently only takes vectors */
    for (int i = 0; i < ResultCount; i++) {  
    TmpVector.push_back( Reservoir[i]->DataItem ); } 
    printf("\nRandomly Selected Samples (ResultCount = %lu): \n", ResultCount);
    PrintVectorData( &TmpVector );
    PrintHistogramSummary( Reservoir, SampleIndex+1 );
    printf("\n");
    
    goto Success;
    
    Success:
        Status = true;
        goto Cleanup;
    Failed:
        Status = false;
        goto Cleanup;
    Cleanup:
        goto Exit;
    Exit:
        return(Status);
}

void PrintHistogramSummary( SAMPLE_ITEM** Reservoir, long ItemsRead )
{
    if ( !Reservoir ) return;

    /*  Allocate buckets */
    BUCKET*  Buckets =  ( BUCKET* ) malloc( BucketCount * 
                                            sizeof( BUCKET ));
    
    if ( !Buckets ) return;
    memset(Buckets, '\0', BucketCount * sizeof(BUCKET));
    
    long BucketSize = ( ItemsRead / BucketCount );
    
    /*  Create buckets  */
    for ( int i = 0; i < BucketCount; i++ ) {
        Buckets[i].MaxValue = BucketSize * (i + 1);
    }
    
    /*  Scan the Reservoir items and use the saved    */
    /*  SampleIndex values to determine which bucket  */
    /*  to update the count  */
    
    for ( int   ReservoirIndex = 0; 
                ReservoirIndex < ResultCount; 
                ReservoirIndex += 1 )
    {
        bool FoundBucket = false;
        
        for ( int   Bucket = 0;
                    Bucket < BucketCount;
                    Bucket += 1 )
        {
            if (    Reservoir[ReservoirIndex]->SampleIndex <= Buckets[Bucket].MaxValue )
            {
                    Buckets[Bucket].Count += 1;
                    FoundBucket = true;
                    break;
            }
        }
        if ( !FoundBucket )
            printf("Could not find bucket for ReservoirIndex = %d\n", ReservoirIndex );
    }
    
    /*  Print the histogram */
    printf("\n");
    printf("Sample Distribution: \n");

    for ( int   Bucket = 0;
                Bucket < BucketCount;
                Bucket += 1)
    {
        printf("Bucket:     %lu     [%lu <-> %lu]\n", 
                Buckets[Bucket].Count, 
                (Buckets[Bucket].MaxValue - BucketSize) + 1,
                Buckets[Bucket].MaxValue);

    }
    return;
}

/*  This function reads a single line from the input      */
/*  text file, parses the columns into data fields        */
/*  into a heap-allocated DATA_ITEM struct, and returns     */
/*  it to the caller, or NULL if we reached EOF or error  */

DATA_ITEM* GetNextDataItem(FILE** FilePtr)
{
    DATA_ITEM*  NewDataItem     = NULL;
    char*       InputLine       = NULL;
    char*       TokenLine       = NULL;
    char*       URL             = NULL;
    size_t      Length          = 0;
    long        LongValue       = 0;
    size_t      BufferSize      = 0;
    ssize_t     BytesRead       = 0;
    char*       Token           = NULL;
    char        Delims[]        = { ' ', '\n', '\0' };   
    short       Column          = 0;
    bool        Status          = false;
    bool        HaveURL         = false;
    bool        HaveValue       = false;
    
    if ( !FilePtr ) return ( NULL );
    
    /* Read the next line from the file pointer  */
    /* the caller provided                       */
    BytesRead = getline(  &InputLine, 
                          &BufferSize, 
                          *FilePtr );
    
    if ( BytesRead < 0 ) return ( NULL );
                
    /* Tokenize the lines from the input file        */
    /* We are making the assumption that the first   */
    /* column of data is a URL string, and the 2nd   */
    /* column is a long integer type, separated by   */
    /* whitespace */
    
    TokenLine = strdup( InputLine );
    
    /*  Loop through the tokens from the line       */    
    for (  Token   = strtok( TokenLine, Delims );
           Token  != NULL;
           Token   = strtok( NULL, Delims ))
    {               
        Column  +=  1;
        switch ( Column )
        {
            case 1:
            
                /* First column should be the URL.           */
                /* We are only doing a very basic check for  */
                /* whether it really is a URL string.        */

                if ( strcasestr( Token, "http" )) { 

                    /* Allocate memory from the heap        */
                    /* to store the URL string, which       */
                    /* will be added to a DATA_ITEM struct  */

                    Length = strlen( Token );
                    
                    URL = ( char* ) malloc( 
                                    sizeof ( char ) * 
                                    ( Length + 1 ));

                    if ( !URL ) {
                        printf("Failed to allocate URL\n");
                        goto Failed;
                    }
                    
                    memset( URL, '\0', sizeof (char) * (Length + 1));
                    strcpy( URL, Token );
                    HaveURL = true;

                } else {

                    printf("Token string is not a URL\n");
                    goto Failed;
                }
                
                break;
                
            case 2:
            
                /*  Second column should be the long value          */
                /*  Just using the stdlib number conversion         */
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

                if  (( LongValue == LONG_MIN )    ||
                     ( LongValue == LONG_MAX )    ||
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
            
        }   /* End column switch */
    }  // End processing line

    /*  If we don't have all the data, fail + cleanup */
    if  (( !HaveURL ) || ( !HaveValue )) 
        goto Failed;
    
    /*  Allocate new struct from the heap to store the data */
    NewDataItem = ( DATA_ITEM* )
                    malloc( sizeof( DATA_ITEM ));

    if  ( !NewDataItem ) {
            printf("Failed to allocate DATA_ITEM\n");
            goto Failed; }

    memset( NewDataItem, '\0', sizeof( DATA_ITEM ));
    
    /*  Fill in the new struct  */
    NewDataItem->URL        = URL;
    NewDataItem->LongValue  = LongValue;

    /*  We are success              */
    /*  Cleanup before returning    */
    goto Success;
    
    Success:
        Status = true;
        goto Cleanup;

    Failed:
        Status = false;
        /*  URL should not be released under   */
        /*  successful executions              */
        if ( URL )
            free ( URL );
        goto Cleanup;

    Cleanup:
        /* Free the memory that getline + strdup allocated */
        if ( InputLine )
            free( InputLine );
        if ( TokenLine )
            free ( TokenLine );
        InputLine = NULL;
        TokenLine = NULL;
        BufferSize = 0;
        goto Exit;
        
    Exit:
        /*  Return the DATA_ITEM struct to the caller   */
        /*  which will either be a valid one, or NULL   */
        return(NewDataItem);
}
    

/*  main  */
int main( int argc, char *argv[] )
{
    printf("\nClickHouse TakeHome v0.1\n\n");
    if ( !ParseArgs( argc, argv )) {
          PrintHelp();
          return (1); }
    
    SORT_COMPARE_FUNCTION   CompareFunction = NULL;
    std::vector             <DATA_ITEM*> DataVector;
    DATA_ITEM*              DataItem        = NULL;
    FILE*                   DataFile        = NULL;
    bool                    Status          = false;
    long                    BeforeLoadTs    = 0;
    long                    AfterLoadTs     = 0;
    long                    BatchLinesRead  = 0;
    long                    BatchesRead     = 0;
    long                    TotalLinesRead  = 0;
    
    CompareFunction = ( ResultSortType == SORT_TYPE_DESCENDING ) ? 
                        CompareDescending : CompareAscending; 

    /*  Generate a test data file if requested */
    if ( GenerateTestDataFile ) { GenerateTestData(
                                  OutputFileName, 
                                  NumLinesToGenerate ); 
                                { printf("\n"); return(0);}}

    /*  Make sure we have an input file specified */
    if ( !InputFileName ) {
        printf("\nIf you want to load an input file, "
               "please specify: -i <Filename> \n\n");
        return (1);
    }

    /* Attempt to open the input file  */
    DataFile = fopen( InputFileName, "r" );
    if ( !DataFile ) {
        printf("Failed to open input file: %s\n", 
                InputFileName );
        goto Failed; }
    
    /* Record the time prior to loading file */
    BeforeLoadTs  =  GetCurrentTimeMs();
    printf( "Loading data from input file: %s\n", InputFileName );
    
    if ( SelectionType == SELECTION_TYPE_RANDOM ) {
        GenerateAlgorithmR( &DataFile );
        goto Exit; }
    
    /*  Begin loading + processing data in batches */
    while ( DataFile )
    {
        BatchLinesRead = 0;
        if ( Verbose ) printf("Start of batch. "
                              "BatchLinesRead = %lu, "
                              "TotalLinesRead = %lu, "
                              "DataVector.size() = %lu\n", 
                               BatchLinesRead, 
                               TotalLinesRead, 
                               DataVector.size());
                               
        /*  Keep reading more lines until we fill   */
        /*  DataVector with a BatchSize amount of   */
        /*  DataItem structs, or if we reached the  */
        /*  end of file and we get a NULL DataItem  */
        while (( DataItem = GetNextDataItem( &DataFile )))
        {
            BatchLinesRead += 1;
            TotalLinesRead += 1;

            /* Add new DATA_ITEM to the DataVector */
            DataVector.push_back ( DataItem );

            if ( Verbose ) 
                printf("Finished line. "
                       " BatchLinesRead = %lu, "
                       " TotalLinesRead = %lu, "
                       " DataVector.size() = %lu\n", 
                       BatchLinesRead, 
                       TotalLinesRead, 
                       DataVector.size());
            
            /*  We've reached the max batch size  */
            /*  so break out of loop              */
            if ( BatchLinesRead == BatchSize )
                break;
    
        }  /* End Reading Batch */
        
        /*  If we are no longer getting DATA_ITEM   */
        /*  data then break out of loop             */ 
        if ( !BatchLinesRead )    
            break;
        
        BatchesRead += 1;
        
        printf("\n");
        printf( "Loaded Batch %lu: "
                "LinesRead = %lu, "
                "TotalRead = %lu, "
                "DataVector.size() = %lu\n", 
                BatchesRead, 
                BatchLinesRead, 
                TotalLinesRead, 
                DataVector.size());
        
        /*  Sort the contents of the data vector which now      */
        /*  contains the addition of a new batch of data.       */        
        /*  Use the default sorting mechanism in C++ with       */
        /*  custom comparator function because our sort key     */
        /*  is a field in a C struct.                           */
    
        /*  It will either the Desc/Asc comparator              */ 
        /*  using function ptr                                  */
        
        sort(   DataVector.begin (), 
                DataVector.end   (), 
                CompareFunction  ); 

        printf("Finished Sorting DataVector\n");
        
        /*  Now trim the DataVector.                        */
        /*  Only keep the ResultCount amount of data        */ 
        /*  in resident Vector memory.                      */
        /*  Start from the tail, freeing struct memory      */
        /*  and removing from DataVector.                   */
        
        if ( DataVector.size() < ResultCount )
            ResultCount = DataVector.size();
        
        for ( long Index = DataVector.size() - 1; 
                   Index > ResultCount - 1;
                   Index -= 1 ){

            DATA_ITEM*  DeleteItem = DataVector[Index];
            if ( DeleteItem->URL )                
                 free ( DeleteItem->URL );
                 free ( DeleteItem );
           
            DataVector.pop_back();
        }

        printf("Finished Trimming DataVector. "
               "DataVector.size() = %lu\n", 
               DataVector.size());
        
        if ( Verbose ) PrintVectorData( &DataVector );
        
        /* Loop back up to do the next batch */
        
    }  /* End Reading File */
    
    AfterLoadTs = GetCurrentTimeMs();
    printf("\n");
    printf("Processed %ld items in %ldms from file: %s\n",
            TotalLinesRead, 
            (AfterLoadTs-BeforeLoadTs), 
            InputFileName );  

    /*  Print the results  */
    printf("\n");
    printf("Top %ld Results ", ResultCount );
    
    if ( ResultSortType == SORT_TYPE_DESCENDING )
        printf("(DESCENDING):\n");
    else if ( ResultSortType == SORT_TYPE_ASCENDING )
        printf("(ASCENDING):\n");
    
    PrintVectorData( &DataVector );

    /*  There are some cleanup items to do before exiting */
    goto Success;

    Success:
        /*  Success, no major failures  */
        Status = true;
        goto Cleanup;

    Failed:
        /*  Sometimes errno shows weird/wrong */
        /*  error text but oh well */
        printf("Error code: %d, text: %s\n", 
                errno, strerror(errno));
        Status = false;
        goto Cleanup;

    Cleanup:    
        /*  Free the rest of the data  */
        while ( !DataVector.empty() ){
          
            DATA_ITEM* DataItem = DataVector.back();
        
            if ( DataItem->URL )                
                 free ( DataItem->URL );
                 free ( DataItem );
                 
            DataVector.pop_back();  
        }
        
        /*  Close input data file  */
        if ( DataFile )
            fclose( DataFile );
        goto Exit;

    Exit:
        printf("\n");
        return(Status);

}

/*  I made two comparators, because I didn't       */
/*  want to potentially slow it down by having an 'if' */
/*  decision for every comparison for Asc/Desc because */
/*  there is a user config option to select what type  */

bool CompareAscending(  DATA_ITEM* Item1,
                        DATA_ITEM* Item2 ){  
return (( Item1->LongValue ) < 
        ( Item2->LongValue ));}

bool CompareDescending( DATA_ITEM* Item1,
                        DATA_ITEM* Item2 ){
return (( Item1->LongValue ) > 
        ( Item2->LongValue ));}


/* Function to print the vector data */
bool PrintVectorData( std::vector<DATA_ITEM*> *DataVector )
{
    // For verbose mode, print out the difference of
    // the LongValues for each array item vs. them
    // previous array item, just to see how far they span
    long PreviousValue = 
        ( (DATA_ITEM*)  DataVector->at(0))->LongValue;

    for ( long Index  =  0;
               Index <  DataVector->size();
               Index += 1 ){

        DATA_ITEM*  Item =  DataVector->at( Index );

        if ( Verbose )
            
            printf( "[%ld] LongValue=%ld (%ld)  URL=%s\n",
                Index, 
                ( Item->LongValue ),
                ( Item->LongValue ) - PreviousValue,
                ( Item->URL   ));
        
        else
            printf( "[%ld] LongValue=%ld  URL=%s\n",
                Index,
                ( Item->LongValue ),
                ( Item->URL   ) );

            
        
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
    
    printf("\n");
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
                /* InputFileName */
                case 'i':
                    if (( arg + 1) < argc ) {
                    InputFileName = argv[( arg + 1 )];}
                    else goto MissingValue;
                    break;
    
                /* ResultCount */
                case 'n':
                    if (( arg + 1) < argc ) {
                        ResultCount = atol( argv[( arg + 1 )] );
                    if (ResultCount <= 0) { goto InvalidValue;}}
                    else goto MissingValue;      
                    break;

                /* BatchSize */
                case 'b':
                    if (( arg + 1) < argc ) {
                        BatchSize = atol( argv[( arg + 1 )] );
                    if (BatchSize <= 0) { goto InvalidValue;}}
                    else goto MissingValue;      
                    break;
                    
                /* SelectionType */
                case 'm':
                    if (( arg + 1) < argc ) {
                        SelectionType = atol( argv[( arg + 1 )] );
                        if ((SelectionType < 0) || (SelectionType > 1))
                        { goto InvalidValue; }}
                    else goto MissingValue;
                    break;
                    
                /* ResultSortType */
                case 's':
                    if (( arg + 1) < argc ) {
                        ResultSortType = atoi( argv[( arg + 1 )]);
                        if ((ResultSortType < 0) || (ResultSortType > 1))
                        { goto InvalidValue; }}
                    else goto MissingValue;
                    break;

                case 'u':
                    if (( arg + 1) < argc ) {
                        BucketCount = atoi( argv[( arg + 1 )]);
                        if (BucketCount < 0) { goto InvalidValue; }}
                    else goto MissingValue;
                    break;
            
                /* OutputFileName for generating test data file */
                case 'o':
                    if (( arg + 1) < argc ) {
                        OutputFileName = argv[( arg + 1 )]; }
                    else goto MissingValue;
                    break;
            
                /* Verbose mode */
                case 'v':
                    Verbose = true;
                    break;
            
                /* GenerateTestData */
                case 'g':
                    if (( arg + 1) < argc ) {
                        GenerateTestDataFile = true;
                        NumLinesToGenerate = atol( argv[( arg + 1 )] );
                        if (NumLinesToGenerate <= 0) { goto InvalidValue;}}
                    else goto MissingValue;
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
    printf("  -i    <Input File>\n\n");
    printf("        Relative or fully qualified path + filename to the input file.\n");
    printf("        Likely if it contains spaces you will need to enclose in quotes.\n");
    printf("\n");
    printf("  -b    <Batch Size>\n\n");
    printf("        Data is processed in batches to conserve memory with large files.\n");
    printf("        The default is 1000 lines per batch.\n");
    printf("\n");
    printf("  -n    <Result Count>\n\n");
    printf("        The default is 10.  Specify a different value if you like. \n");
    printf("\n");
    printf("  -u    <Bucket Count>\n\n");
    printf("        Applies to Random/Sampling mode.  Specifies the number of \n");
    printf("        Histogram Buckets used in the post-generation report\n");
    printf("\n");    
    printf("  -s    <Result Sort Type>\n\n");
    printf("            0 = Descending\n");
    printf("            1 = Ascending\n");
    printf("        The default is 0.\n");
    printf("\n");
    printf("  -m    <Selection Mode>\n\n");
    printf("        Specifies method selecting lines for 'Top' results.\n");
    printf("            0 = Normal mode. Result is the sorted Top N of all batches.\n");
    printf("            1 = Random/Sampling mode.\n");
    printf("        Default is 0 / Normal mode.\n");
    printf("\n");
    printf("  -g  <Generate Test Data>\n\n");
    printf("      This will generate a Test Data File with random values.\n");
    printf("      '-g 50000' will enable the creation of a test data file\n");
    printf("      with 50,000 lines of URLs and Long numbers.  It is not enabled by default.\n");
    printf("\n");
    printf("  -o  <Test Data Output File>\n\n");
    printf("      The name of the Test Data file if you are generating one.\n");
    printf("\n");
    printf("  -v  <Verbose Output>\n\n");
    printf("      Default is non-verbose\n");

    return;
}

