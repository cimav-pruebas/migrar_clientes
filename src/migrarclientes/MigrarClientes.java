/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package migrarclientes;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author juan.calderon
 */
public class MigrarClientes {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        migrarClientes();
    }
    
    private static void migrarClientes() {
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();
            
            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connOracle = DriverManager.getConnection("jdbc:oracle:thin:@//10.1.0.44:1521/cimavXDB.netmultix.cimav.edu.mx", "almacen", "afrika");
                                                               //jdbc:oracle:thin:@//10.1.0.44:1521/cimavXDB.netmultix.cimav.edu.mx
            Connection connPostgres = DriverManager.getConnection("jdbc:postgresql://10.0.4.40:5432/rh_development", "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPost = connPostgres.createStatement()) {
                String sqlClientes = "SELECT * FROM CL01";
                ResultSet rsOraClientes = stmtOra.executeQuery(sqlClientes);
                
                while(rsOraClientes.next()){
                    String clave = rsOraClientes.getString("CL01_CLAVE").trim();
                    String rfc = rsOraClientes.getString("CL01_RFC").trim();
                    String nom1 = rsOraClientes.getString("CL01_NOMBRE").trim();
                    String calle = rsOraClientes.getString("CL01_CALLE").trim();
                    String colonia = rsOraClientes.getString("CL01_COLONIA").trim();
                    Integer idCiudad = rsOraClientes.getInt("CL01_CIUDAD");
                    String pais = rsOraClientes.getString("CL01_Pais").trim();
                    String postal = rsOraClientes.getString("CL01_postal").trim();
                    String lada = rsOraClientes.getString("CL01_lada").trim();
                    String tel1 = rsOraClientes.getString("CL01_telefono1").trim();
                    String tel2 = rsOraClientes.getString("CL01_telefono2").trim();
                    String fax = rsOraClientes.getString("CL01_fax").trim();
                    String contacto = rsOraClientes.getString("CL01_contacto").trim();
                    Integer idTipoCliente = rsOraClientes.getInt("CL01_tipo_cliente");
                    Integer idTipoEmpresa = rsOraClientes.getInt("CL01_tipo_empresa");
                    Integer numEmp = rsOraClientes.getInt("CL01_empleados");
                    String nom2 = rsOraClientes.getString("CL01_NOMBRE2").trim();
                    String localidad = rsOraClientes.getString("CL01_localidad").trim();
                    String num_ext = rsOraClientes.getString("CL01_no_ext").trim();
                    String num_int = rsOraClientes.getString("CL01_no_int").trim();
                    Integer idEstado = rsOraClientes.getInt("CL01_estado");
                    
                    String nombre = nom1 + nom2;
                    calle = calle + " " + num_ext + " " + num_int;
                    if (!localidad.isEmpty()) colonia = colonia + ", " + localidad;
                    String telefono = lada + " " + tel1 + " " + tel2;
                    fax = lada + " " + fax;
                    
                    
                    String slqMigration = clave + ", " + contacto;
                    
                    if (contacto.trim().length() > 0)
                        System.out.println("" + slqMigration);

                }
                
            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connOracle.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarClientes.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
