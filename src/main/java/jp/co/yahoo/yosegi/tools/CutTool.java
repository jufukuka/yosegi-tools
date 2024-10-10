package jp.co.yahoo.yosegi.tools;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.formatter.IStreamWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.reader.YosegiSchemaReader;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class CutTool {
    private CutTool() {}

    public static Options createOptions( final String[] args ){

        Option input = OptionBuilder.
            withLongOpt("input").
            withDescription("Input file paths.").
            hasArg().
            isRequired().
            withArgName("input").
            create( 'i' );

        Option ppd = OptionBuilder.
            withLongOpt("projection_pushdown").
            withDescription("Use projection pushdown. Format:\"[ [ \"column1\" , \"[column1-child]\" , \"column1-child-child\" ] [ \"column2\" , ... ] ... ]\"").
            hasArg().
            withArgName("projection_pushdown").
            create( 'p' );

        Option flatten = OptionBuilder.
            withLongOpt("flatten").
            withDescription("Use flatten function.").
            hasArg().
            withArgName("flatten").
            create( 'x' );

        Option output = OptionBuilder.
            withLongOpt("output").
            withDescription("output file path. \"-\" standard output").
            hasArg().
            isRequired().
            withArgName("output").
            create( 'o' );

        Option more = OptionBuilder.
            withLongOpt("more").
            withDescription("or more").
            hasArg().
            isRequired().
            withArgName("more").
            create( 'm' );

        Option less = OptionBuilder.
            withLongOpt("less").
            withDescription("or less").
            hasArg().
            isRequired().
            withArgName("less").
            create( 'l' );

        Option help = OptionBuilder.
            withLongOpt("help").
            withDescription("help").
            withArgName("help").
            create( 'h' );

        Options  options = new Options();

        return options
            .addOption( input )
            .addOption( ppd )
            .addOption( flatten )
            .addOption( output )
            .addOption( more )
            .addOption( less )
            .addOption( help );
    }

    public static void printHelp( final String[] args ){
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp( "[options]" , createOptions( args ) );
    }

    public static int run( final String[] args ) throws IOException{
        CommandLine cl;
        try{
            CommandLineParser clParser = new GnuParser();
            cl = clParser.parse( createOptions( args ) , args );
        }catch( ParseException e ){
            printHelp( args );
            throw new IOException( e );
        }

        if( cl.hasOption( "help" ) ){
            printHelp( args );
            return 0;
        }

        String input = cl.getOptionValue( "input" , null );
        String output = cl.getOptionValue( "output" , null );
        String ppd = cl.getOptionValue( "projection_pushdown" , null );
        String flatten = cl.getOptionValue( "flatten" , null );
        int more = Integer.valueOf(cl.getOptionValue("more", "0"));
        int less = Integer.valueOf(cl.getOptionValue("less", "0"));
        if (more >= less) {
            less = more;
        }

        OutputStream out = FileUtil.create( output );

        Configuration config = new Configuration();
        if( ppd != null ){
            config.set( "spread.reader.read.column.names" , ppd );
        }

        if( flatten != null ){
            config.set( "spread.reader.flatten.column" , flatten );
        }

        YosegiReader reader = new YosegiReader();
        YosegiWriter writer = new YosegiWriter( out , config );

        int total = 0;
        List<File> mergeList = FileUtil.pathToFileList( input );
        for( File file : mergeList ){
            InputStream in = FileUtil.fopen( file );
            long fileLength = file.length();
            reader.setNewStream( in , fileLength , config );
            while( reader.hasNext() ){
                //writer.appendRow( reader.nextRaw() , reader.getCurrentSpreadSize() );
                List<ColumnBinary> raw = reader.nextRaw();
                int spreadSize = reader.getCurrentSpreadSize();
                total += spreadSize;
                System.out.println(total);
                if (total >= more) {
                    System.out.printf("Add: %d\n", total);
                    writer.appendRow(raw, spreadSize);
                }
                if (total >= less) {
                    break;
                }
            }
            reader.close();
        }
        writer.close();
        System.out.printf("TOTAL: %d\n", total);

        return 0;
    }

    public static void main( final String[] args ) throws IOException{
        System.exit( run( args ) );
    }
}
