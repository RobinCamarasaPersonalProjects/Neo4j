import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import static org.neo4j.driver.v1.Values.parameters;

public class Principal implements AutoCloseable
{
    private final Driver driver;

    public Principal(String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void createPerson(final String name, final String sex)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "CREATE (a:Person) " +
                                    "SET a.name = $name " +
                                    "SET a.sex = $sex " +
                                    "RETURN a.name + ', from node ' + id(a)",
                            parameters( "name", name, "sex", sex ) );
                    return result.single().get( 0 ).asString();
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createUP(final String name, final Integer volume)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "CREATE (a:UP) " +
                                    "SET a.name = $name " +
                                    "SET a.nb_hours = $volume " +
                                    "RETURN a.name + ', from node ' + id(a)",
                            parameters( "name", name, "volume", volume ) );
                    return result.single().get( 0 ).asString();
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createGP(final String name, final Integer volume)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "CREATE (a:GP) " +
                                    "SET a.name = $name " +
                                    "SET a.nb_hours = $volume " +
                                    "RETURN a.name + ', from node ' + id(a)",
                            parameters( "name", name, "volume", volume ) );
                    return result.single().get( 0 ).asString();
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createLoveRelation(final String name1, final String name2)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (node1:Person {name: $name1}) " +
                                    "MATCH (node2:Person {name: $name2}) " +
                                    "CREATE (node1)-[:LOVES]->(node2)",
                            parameters( "name1", name1, "name2", name2 ) );
                    return (name1 + " loves " + name2);
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createFriendRelation(final String name1, final String name2)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (node1:Person {name: $name1}) " +
                                    "MATCH (node2:Person {name: $name2}) " +
                                    "CREATE (node1)-[:IS_FRIEND_WITH]->(node2)",
                            parameters( "name1", name1, "name2", name2 ) );
                    return (name1 + " is friend with " + name2);
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createAttendsRelation(final String assignement_name, final String person_name)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (node1:Person {name: $name1}) " +
                                    "MATCH (node2:UP {name: $name2}) " +
                                    "CREATE (node1)-[:ATTENDS]->(node2)",
                            parameters( "name1", person_name, "name2", assignement_name ) );
                    return (person_name + " atttends " + assignement_name);
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createTeachRelation(final String assignement_name, final String person_name)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (node1:Person {name: $name1}) " +
                                    "MATCH (node2:UP {name: $name2}) " +
                                    "CREATE (node1)-[:TEACHES]->(node2)",
                            parameters( "name1", person_name, "name2", assignement_name ) );
                    return (person_name + " teaches " + assignement_name);
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createManageRelation(final String assignement_name, final String person_name)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (node1:Person {name: $name1}) " +
                                    "MATCH (node2:GP {name: $name2}) " +
                                    "CREATE (node1)-[:MANAGES]->(node2)",
                            parameters( "name1", person_name, "name2", assignement_name ) );
                    return (person_name + " manages " + assignement_name);
                }
            } );
            System.out.println( greeting );
        }
    }

    public void createIsPartRelation(final String up, final String gp)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (node1:UP {name: $up}) " +
                                    "MATCH (node2:GP {name: $gp}) " +
                                    "CREATE (node1)-[:IS_PART_OF]->(node2)",
                            parameters( "up", up, "gp", gp ) );
                    return (up + " is part of " + gp);
                }
            } );
            System.out.println( greeting );
        }
    }

    public void clear()
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (n) DETACH DELETE n",
                            parameters( ));
                    return "Database cleared";
                }
            } );
            System.out.println( greeting );
        }
    }

    public static void main( String... args ) throws Exception
    {
        try ( Principal principal = new Principal( "bolt://localhost:7687", "neo4j", "test" ) )
        {
            principal.clear();
            principal.createPerson( "Jeanne", "woman" );
            principal.createPerson( "Mihaela", "woman" );
            principal.createPerson( "Robin", "man" );
            principal.createPerson( "Sylvain", "man" );
            principal.createPerson( "Maxime", "man" );
            principal.createUP("Données massives", 30);
            principal.createGP("Big Data", 160);
            principal.createLoveRelation("Jeanne", "Robin");
            principal.createLoveRelation("Robin", "Jeanne");
            principal.createFriendRelation("Sylvain", "Robin");
            principal.createFriendRelation("Robin", "Sylvain");
            principal.createTeachRelation("Données massives", "Maxime");
            principal.createTeachRelation("Données massives", "Mihaela");
            principal.createAttendsRelation("Données massives", "Robin");
            principal.createAttendsRelation("Données massives", "Sylvain");
            principal.createManageRelation("Big Data", "Mihaela");
            principal.createIsPartRelation("Données massives", "Big Data");
        }
    }
}