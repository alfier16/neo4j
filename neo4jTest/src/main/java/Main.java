/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import javax.swing.JOptionPane;
import org.neo4j.driver.v1.*;
/**
 *
 * @author Javier
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

//        Driver driver = GraphDatabase.driver("bolt://localhost",AuthTokens.basic("neo4j","javier"), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
//        Session session = driver.session();
//        session.run("CREATE (a:person{name:'Javier',title:'king'})");
//        
//        StatementResult result = session.run("MATCH (a:person) WHERE a.name= 'Javier' RETURN a.name AS name, a.title AS title"); 
//        while (result.hasNext()){
//            Record record = result.next();
//            System.out.println(record.get("title").asString()+" "+record.get("name").asString());
//        }
//crearArbol("neo4j","javier","select * from base where f1=3 and f4=5");


//        session.close();
//        driver.close();
    }
    
    public static int crearArbol(String usr,String pass,String consulta,String host ){
        
        int res = validaConsulta(consulta);
        if (res == 1){
            //System.out.println("La consulta no es valida");
            //System.exit(res);
            return 2;
        }
try{
        Driver driver = GraphDatabase.driver("bolt://"+host,AuthTokens.basic(usr,pass), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
        Session session = driver.session();
        //session.run("CREATE (consulta:incio{name:'Javier',title:'king'})");



        session.run("match(n) detach delete n");
        session.run("CREATE (:inicio{name:'INICIO'})");      
//        StatementResult result = session.run("MATCH (a:person) WHERE a.name= 'Javier' RETURN a.name AS name, a.title AS title"); 
//        while (result.hasNext()){
//            Record record = result.next();
//            System.out.println(record.get("title").asString()+" "+record.get("name").asString());
//        }

String [] parts = consulta.split(" ");
String cadena = ""; 

        for (int x=0; x<parts.length;x++ ){
            if (x == 1){
            String [] fields = parts[x].split(",");
                for (int i=0; i<fields.length; i++){
                    session.run("CREATE(:fields{name:'"+fields[i].toUpperCase()+"'}) ");      
                }
            }
            else if (x==3){
            session.run("CREATE(:bd{name:'"+parts[x].toUpperCase()+"'})");      
            }
        }//end for
        for (int x=0; x<parts.length;x++ ){
            if (parts[x].toUpperCase().equals("SELECT")){
            session.run("MATCH(i:inicio) MATCH(f:fields) CREATE(i)-[:"+ parts[x].toUpperCase()+"{name:'"+ parts[x].toUpperCase()+"'}]->(f) ");            
            }
            else if (parts[x].toUpperCase().equals("FROM") ){
            session.run("MATCH(f:fields) MATCH(b:bd) CREATE(f)-[:"+ parts[x].toUpperCase()+"{name:'"+ parts[x].toUpperCase()+"'}]->(b) ");         
            }
        }
   String [] nodosWhere = obtenNodos(consulta);
        for (int j=0; j<parts.length;j++ ){
            if (parts[j].toUpperCase().equals("WHERE")){

                int flag=0;
            for(int x=0; x<nodosWhere.length;x++ ){
                
               if (nodosWhere[x].toUpperCase().equals("=") || nodosWhere[x].toUpperCase().equals("<") || nodosWhere[x].toUpperCase().equals(">") || nodosWhere[x].toUpperCase().equals("<=") || nodosWhere[x].toUpperCase().equals(">=") || nodosWhere[x].toUpperCase().equals("!=")){
                   session.run("MERGE(:operador{name:'"+nodosWhere[x].toUpperCase().trim()+"'})");
                   session.run("MERGE(:condicion{name:'"+nodosWhere[x+1].toUpperCase().trim()+"'})");
                   flag=1;
               }
               else if (nodosWhere[x].toUpperCase().equals("AND") || nodosWhere[x].toUpperCase().equals("OR")){}
               else{
                   if(flag!=1){
                    session.run("MERGE(f:fields{name:'"+nodosWhere[x].toUpperCase().trim()+"'})");
                   }
                   else {flag=0;}
               }
            }
            
            }
        
        
        }
        

        
        for (int x=0; x<parts.length;x++ ){
            if (parts[x].toUpperCase().equals("WHERE")){
                int bandera=0;
            for(int n=0; n<nodosWhere.length-1;n++ ){
               if (nodosWhere[n].toUpperCase().equals("=") || nodosWhere[n].toUpperCase().equals("<") || nodosWhere[n].toUpperCase().equals(">") || nodosWhere[n].toUpperCase().equals("<=") || nodosWhere[n].toUpperCase().equals(">=") || nodosWhere[n].toUpperCase().equals("!=")){
                   session.run("MATCH(o:operador{name:'"+nodosWhere[n].toUpperCase().trim()+"'}) MATCH(c:condicion{name:'"+nodosWhere[n+1].toUpperCase().trim()+"'}) CREATE(o)-[:that{name:'condicion'}]->(c)");
               }
               else if (nodosWhere[n].toUpperCase().equals("AND") || nodosWhere[n].toUpperCase().equals("OR")){
                   session.run("MATCH(b:bd) MATCH(f:fields{name:'"+nodosWhere[n+1].toUpperCase().trim()+"'}) CREATE(b)-[:"+nodosWhere[n].toUpperCase().trim()+"_where{name:'condicion'}]->(f)");
                   bandera=1;
                   
               }
               else{
                   session.run("MATCH(f:fields{name:'"+nodosWhere[n].toUpperCase().trim()+"'}) MATCH(o:operador{name:'"+nodosWhere[n+1].toUpperCase().trim()+"'}) CREATE(f)-[:is{name:'sea'}]->(o)");
                   if(bandera!=1){
                   session.run("MATCH(b:bd) MATCH(f:fields{name:'"+nodosWhere[n].toUpperCase().trim()+"'}) CREATE(b)-[:where{name:'condicion'}]->(f)"); 
                   bandera=0;
                   }
               }
            }


            }
        }
       
        //System.out.print(cadena);
        
        session.close();
        driver.close();        
        return 1;
        }catch(Exception e){return 3;}
    }
    
    public static int validaConsulta(String consulta){
        int res=0;
        if(consulta.toUpperCase().indexOf("SELECT")<0 || consulta.toUpperCase().indexOf("FROM")<0 ){
            res=1;
        }            
    return res;    
    }//Fin valida consulta

    private static String[] obtenNodos(String consulta) {
                String condiciones;
       int posSelect = consulta.toUpperCase().indexOf("WHERE");
        condiciones=consulta.substring(posSelect+5).trim().toUpperCase();
        
        condiciones= condiciones.replaceAll("AND",",AND,");
        condiciones= condiciones.replaceAll("OR", ",OR,");
        if (condiciones.toUpperCase().indexOf("=")>0 ){ condiciones=condiciones.replace("=", ",=,"); }
        if (condiciones.toUpperCase().indexOf(">")>0){ condiciones=condiciones.replace(">", ",>,"); }
        if (condiciones.toUpperCase().indexOf("<")>0){ condiciones=condiciones.replace("<", ",<,"); }
        if (condiciones.toUpperCase().indexOf(">=")>0){ condiciones=condiciones.replace(">=", ",>=,"); }
        if (condiciones.toUpperCase().indexOf("<=")>0){ condiciones=condiciones.replace("<=", ",<=,"); }
        if (condiciones.toUpperCase().indexOf("!=")>0){ condiciones=condiciones.replace("!=", ",!=,"); }
        String [] nodos = condiciones.split(",");        
        for(int i=0;i<nodos.length;i++){
        nodos[i]= nodos[i].trim();
        }
        return nodos;
    }
    
}
