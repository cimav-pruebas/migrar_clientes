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
            Connection connPostgres = DriverManager.getConnection("jdbc:postgresql://localhost:5432/sigre_development");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPost = connPostgres.createStatement()) {
                
                /****************/
                stmtPost.execute("DELETE from vinculacion_clientes");
                stmtPost.execute("ALTER SEQUENCE vinculacion_clientes_id_seq RESTART WITH 1");
                /****************/
                
                String sqlClientes = "SELECT * FROM CL01 c1 JOIN CL13 c13 ON c1.CL01_CIUDAD = c13.CL13_CIUDAD";
                
                ResultSet rsOraClientes = stmtOra.executeQuery(sqlClientes);
                
                while(rsOraClientes.next()){
                    String clave = rsOraClientes.getString("CL01_CLAVE").trim();
                    String rfc = rsOraClientes.getString("CL01_RFC").trim();
                    String nom1 = rsOraClientes.getString("CL01_NOMBRE").trim();
                    String calle = rsOraClientes.getString("CL01_CALLE").trim();
                    String colonia = rsOraClientes.getString("CL01_COLONIA").trim();
                    Integer idCiudad = rsOraClientes.getInt("CL01_CIUDAD");
                    String idPais = rsOraClientes.getString("CL01_Pais").trim();
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
                    
                    String ciudad = rsOraClientes.getString("CL13_nombre").trim();
                    
                    String nombre = nom1 + nom2;
                    calle = calle + " " + num_ext + " " + num_int;
                    if (!localidad.isEmpty()) colonia = colonia + ", " + localidad;
                    String telefono = lada + " " + tel1 + " " + tel2;
                    fax = lada + " " + fax;
                    
                    idTipoCliente = idTipoCliente == 0 ? 6 : idTipoCliente; // tipoCliente es el SECTOR :: 1EMPRESA, 2SECTOR EDUCATIVO, 3CENTRO..., 4PUBLi..., 5MAQUILA    0y6 son Ninugno
                    
                    String sqlMigration = "clave, nombre, rfc, razon_social, num_empleados, calle_num, colonia, cp, telefono, fax, created_at, tamano_empresa, sector, pais_id, estado_id, ciudad";
                    String sqlValues = "'" + clave.trim() + "', '" + nombre + "', '" + rfc.trim() + "', '" + nombre  + "', " + numEmp + ", '" + calle + "', '" + colonia + "', '" + postal.trim() + "', '" 
                            + telefono.trim() + "', '" + fax.trim() + "', now(), " + idTipoEmpresa  + ", " + idTipoCliente + ", " + idPais + ", " + idEstado + ", '" + ciudad + "'";
                    
                    sqlMigration = "INSERT INTO vinculacion_clientes (" + sqlMigration + ") VALUES (" + sqlValues + ");";
                            
                    System.out.println("" + sqlMigration);

                    /****************/
                    stmtPost.execute(sqlMigration);
                    /****************/
                    
// select table_name, column_name from dba_tab_columns where column_name like '%CIUDAD%';
                    
// INSERT INTO "public".vinculacion_clientes (rfc, razon_social, num_empleados, calle_num, colonia, cp, telefono, fax, email, created_at, updated_at, tamano_empresa, sector, pais_id, estado_id, ciudad, clave, nombre) 
// vALUES ('COGJ750304QN3', 'JESUS MIGUEL COBOS GALLARDO', 0, 'FILTEPEC 414 ', 'CAFETALES , CHIHUAHUA', '31125', '  ', ' ', NULL, NULL, NULL, 0, 1, 1, 8, 'CHIHUAHUA', '00445', 'JESUS MIGUEL COBOS GALLARDO');
                    
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
