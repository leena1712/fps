package com.dao;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.FlushMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;

import com.github.dandelion.core.utils.StringUtils;
import com.github.dandelion.datatables.core.ajax.ColumnDef;
import com.github.dandelion.datatables.core.ajax.DataSet;
import com.github.dandelion.datatables.core.ajax.DatatablesCriterias;
import com.models.Location;
import com.models.Soil;
import com.models.SoilAttribute;
import com.models.soil_health;
import com.persistance.util.HibernateUtil;
import com.persistance.util.HibernateUtilNA;

import connection.ConnectionProvider;

public class SoilDAOImpl implements SoilDAO {

	@Override
	public List<Soil> getSoilReport() {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("from Soil");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
	}
	@Override
	public List<Soil> getsur_form(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(sur_form) from Soil where sur_form is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil getsur_formZoom(String sur_form) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where sur_form= '"+sur_form+"'");
	
		List<Soil> list = (List<Soil>) q.list();
	
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
			
			return db;
		}
	
	@Override
	public List<Soil> getsoil_depth(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(soil_depth) from Soil where soil_depth is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoildistrict(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(district) from soil_health ");			
		System.out.print("district.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_n_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(n_rating) from soil_health ");			
		System.out.print("n_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_p_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(p_rating) from soil_health ");			
		System.out.print("p_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_ec_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(ec_rating) from soil_health ");			
		System.out.print("ec_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_ph_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(ph_rating) from soil_health ");			
		System.out.print("ph_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_k_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(k_rating) from soil_health ");			
		System.out.print("k_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_oc_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(oc_rating) from soil_health ");			
		System.out.print("oc_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	@Override
	public List<soil_health> getsoil_zn_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(zn_rating) from soil_health ");			
		System.out.print("zn_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_fe_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(fe_rating) from soil_health ");			
		System.out.print("fe_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	@Override
	public List<soil_health> getsoil_cu_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(cu_rating) from soil_health ");			
		System.out.print("cu_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_mn_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(mn_rating) from soil_health ");			
		System.out.print("mn_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@Override
	public List<soil_health> getsoil_b_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(b_rating) from soil_health ");			
		System.out.print("b_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	@Override
	public List<soil_health> getsoil_s_rating(){
		
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(s_rating) from soil_health ");			
		System.out.print("s_rating.."+q);
		@SuppressWarnings("unchecked")
		List<soil_health> list = (List<soil_health>) q.list();
		
		tx.commit();
		return list;
		
	}
	@SuppressWarnings("unchecked")
	@Override
	public Soil getsoil_depthZoom(String soil_depth) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where soil_depth= '"+soil_depth+"'");
		
		List<Soil> list = (List<Soil>) q.list();
	
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
			
			return db;
		}
	
	
	@Override
	public List<Soil> getsoil_react(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(soil_react) from Soil where soil_react is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil getsoil_reactZoom(String soil_react) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
	
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where soil_react= '"+soil_react+"'");
		
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
		
			return db;
		}
	
	@Override
	public List<Soil> getsoil_drain(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(soil_drain) from Soil where soil_drain is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil getsoil_drainZoom(String soil_drain) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where soil_drain= '"+soil_drain+"'");
		
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		session.close();
		
		
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
			
			return db;
		}
	
	
	@Override
	public List<Soil> getsoil_text(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(soil_text) from Soil where soil_text is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil getsoil_textZoom(String soil_text) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where soil_text= '"+soil_text+"'");
		
		List<Soil> list = (List<Soil>) q.list();
	
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
			
			return db;
		}
	
	@Override
	public List<Soil> geterosion(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(erosion) from Soil where erosion is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil geterosionZoom(String erosion) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where erosion= '"+erosion+"'");
		
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
			
			return db;
		}
	
	@Override
	public List<Soil> getslope(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(slope) from Soil where slope is not null");			
		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@Override
	public List<SoilAttribute> getsalinity(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(salinity) from SoilAttribute where salinity is not null");			
		
		@SuppressWarnings("unchecked")
		List<SoilAttribute> list = (List<SoilAttribute>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	@Override
	public List<SoilAttribute> getsodicity(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(sodicity) from SoilAttribute where sodicity is not null");			
		
		@SuppressWarnings("unchecked")
		List<SoilAttribute> list = (List<SoilAttribute>) q.list();
	
		tx.commit();
		return list;
		
	}
	
	@Override
	public List<SoilAttribute> getacidity(){
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
		Query q = session.createQuery("select distinct(acidity) from SoilAttribute where acidity is not null");			
		
		@SuppressWarnings("unchecked")
		List<SoilAttribute> list = (List<SoilAttribute>) q.list();
		
		tx.commit();
		return list;
		
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil getslopeZoom(String slope) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where slope= '"+slope+"'");
	
		List<Soil> list = (List<Soil>) q.list();
	
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
		
			return db;
		}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<Soil> getSoilQueryReport(String cqlquery,String tbname) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();	
	
		String cql = null;
		try {
			 cql=URLEncoder.encode(cqlquery, "UTF-8");
	
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Query q = session.createQuery("from Soil where "+cqlquery);			
		
		
		List<Soil> list = (List<Soil>) q.list();
	
		tx.commit();
		return list;
	}
	
	
	
	@Override
	public List<Map<String, Object>> getSoilQueryReport_all (String cqlquery,String tbname)
	{
		
		    ResultSet rs = null;
		    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		    Connection conn = null;
			   try{
				   conn=ConnectionProvider.getConnection();
		
		        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM "+tbname+" where "+cqlquery +" limit 100");
		   
		        rs = stmt.executeQuery();
		    
		        
		        Map<String, Object> columns = new LinkedHashMap<String, Object>();
		        ResultSetMetaData metaData = rs.getMetaData();
		        int columnCount = metaData.getColumnCount();

		        while (rs.next()) {
		          

		            for (int i = 1; i <= columnCount; i++) {
		            	System.out.println("col name=="+metaData.getColumnLabel(i));
		            	 if(metaData.getColumnLabel(i).equals("geom") || metaData.getColumnLabel(i).equals("minx") || metaData.getColumnLabel(i).equals("miny") ||metaData.getColumnLabel(i).equals("maxx") || metaData.getColumnLabel(i).equals("maxy") || metaData.getColumnLabel(i).equals("centx") || metaData.getColumnLabel(i).equals("centy")){
		            		 columns.remove(metaData.getColumnLabel(i));
		              
		           	 }
		            	 
		            	 else
		            	 {
		            		 
		            		  columns.put(metaData.getColumnLabel(i), rs.getObject(i));
		            	 }
		            }

		            rows.add(columns);
		         
		        }
		   
		        
		        
		        
		    } catch (SQLException e){
		        e.printStackTrace();
		    }
		    return rows;  
		}
	
	@SuppressWarnings("unchecked")
	@Override
	public Soil getSoilqueryzoomBoundary(String cqlquery) {
		Session session= HibernateUtilNA.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
	
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("from Soil where "+cqlquery);
		
		List<Soil> list = (List<Soil>) q.list();
		
		tx.commit();
		session.close();
		
		
			
			Double minxx = null;
			Double minyy = null;
			Double maxxx = null;
			Double maxyy = null;

			ArrayList <Double> min_x = new ArrayList<Double>();
			ArrayList <Double> min_y = new ArrayList<Double>();
			ArrayList <Double> max_x = new ArrayList<Double>();
			ArrayList <Double> max_y = new ArrayList<Double>();
			
			if(list.size()>0)
			{	
				Iterator it1 =list.iterator();
				Iterator it2 =list.iterator();
				Iterator it3 =list.iterator();
				Iterator it4 =list.iterator();
				
				while (it1.hasNext() && it2.hasNext() && it3.hasNext() && it4.hasNext()) {

				min_x.add(((Soil) it1.next()).getMinx());
				min_y.add(((Soil) it2.next()).getMiny());
				max_x.add(((Soil) it3.next()).getMaxx());
				max_y.add(((Soil) it4.next()).getMaxy());
				
				}
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				

			} 
			Soil db = new Soil();
			db.setMinx(minxx);
			db.setMiny(minyy);
			db.setMaxx(maxxx);
			db.setMaxy(maxyy);
			
			return db;
		}
	
	
	public DataSet<Soil> findSoilDetailWithDatatablesCriterias(DatatablesCriterias criterias, int userID) {
				
			System.out.println("Soil Report");
			List<Soil> metadata = findSoilDetailCriteriasAdmin(criterias);
			Long count = getTotalCountAdmin();
			Long countFiltered = getFilteredCountAdmin(criterias);
			return new DataSet<Soil>(metadata, count, countFiltered);
	}
	
	@SuppressWarnings("unchecked")
	private List<Soil> findSoilDetailCriteriasAdmin(DatatablesCriterias criterias) {
		StringBuilder queryBuilder = new StringBuilder("SELECT d from  Soil d ");
		queryBuilder.append(getFilterQueryForAdmin(criterias));
		if (criterias.hasOneSortedColumn()) {
			List<String> orderParams = new ArrayList<String>();
			queryBuilder.append(" ORDER BY ");
			for (ColumnDef columnDef : criterias.getSortingColumnDefs()) {
				if (columnDef.getName().contains("hodProfile.fullName")) {
					orderParams.add("d.hodProfile.firstName"
							+ columnDef.getSortDirection());
				} else {
					orderParams.add("d." + columnDef.getName() + " "
							+ columnDef.getSortDirection());
				}
			}

			Iterator<String> itr2 = orderParams.iterator();
			while (itr2.hasNext()) {
				queryBuilder.append(itr2.next());
				if (itr2.hasNext()) {
					queryBuilder.append(" , ");
				}
			}
		}
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery(queryBuilder.toString());
		q.setFirstResult(criterias.getDisplayStart());
		q.setMaxResults(criterias.getDisplaySize());
		List<Soil> list = (List<Soil>) q.list();
		tx.commit();
		session.close();
		return list;
	}
	
	private static StringBuilder getFilterQueryForAdmin(DatatablesCriterias criterias) {
		StringBuilder queryBuilder = new StringBuilder();
		List<String> paramList = new ArrayList<String>();
		if (StringUtils.isNotBlank(criterias.getSearch()) && criterias.hasOneFilterableColumn()) {
			queryBuilder.append(" WHERE");
			//queryBuilder.append(" OR ");
			for (ColumnDef columnDef : criterias.getColumnDefs()) {
				if (columnDef.isFilterable() && StringUtils.isBlank(columnDef.getSearch())) {
					System.out.println("columnDef.getName().equalsIgnoreCase()===");
					if (columnDef.getName().equalsIgnoreCase("gid")
							) {
						
						System.out.println(" if ");
						
						if (criterias.getSearch().matches("[0-9]+")) {
							System.out.println(" if1 ");
							paramList.add(" d."	+ columnDef.getName()+ " = '?'".replace("?", criterias.getSearch().toLowerCase()));
						}
					} else {
						System.out.println(" else ");
						paramList.add(" LOWER(d."+ columnDef.getName()+ ") LIKE '?%'".replace("?", criterias.getSearch().toLowerCase()));
					}
				}
			}

			Iterator<String> itr = paramList.iterator();
			while (itr.hasNext()) {
				queryBuilder.append(itr.next());
				if (itr.hasNext()) {
					queryBuilder.append(" OR ");
				}
			}
		}
		if (criterias.hasOneFilterableColumn() && criterias.hasOneFilteredColumn()) {
			paramList = new ArrayList<String>();

			if (!queryBuilder.toString().contains("WHERE")) {
				queryBuilder.append(" WHERE ");
			} else {
				queryBuilder.append(" AND ");
			}
			for (ColumnDef columnDef : criterias.getColumnDefs()) {
				if (columnDef.isFilterable()) {
					if (StringUtils.isNotBlank(columnDef.getSearch())) {
						if (columnDef.getName().equalsIgnoreCase("gid")
								|| columnDef.getName().equalsIgnoreCase("srm_id")
								|| columnDef.getName().equalsIgnoreCase("sur_form")
								|| columnDef.getName().equalsIgnoreCase("soil_depth")
								|| columnDef.getName().equalsIgnoreCase("soil_react")
								|| columnDef.getName().equalsIgnoreCase("soil_drain")
								|| columnDef.getName().equalsIgnoreCase("soil_text")
								|| columnDef.getName().equalsIgnoreCase("erosion")
								|| columnDef.getName().equalsIgnoreCase("slope")) {
							if (criterias.getSearch().matches("[0-9]+")) {
								paramList.add(" d."+ columnDef.getName()+ " = '?'".replace("?", criterias.getSearch().toLowerCase()));
							}
						} else {
							paramList.add(" LOWER(d."+ columnDef.getName()+ ") LIKE '%?%'".replace("?", columnDef.getSearch().toLowerCase()));
						}
					}
				}
			}

			Iterator<String> itr = paramList.iterator();
			while (itr.hasNext()) {
				queryBuilder.append(itr.next());
				if (itr.hasNext()) {
					queryBuilder.append(" AND ");
				}
			}
		}
		return queryBuilder;
	}
	private Long getTotalCountAdmin() {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery("SELECT COUNT(d) FROM Soil d ");
		Long count = (Long) q.list().get(0);
		tx.commit();
		session.close();
		return count;
	}
	@SuppressWarnings("unchecked")
	private Long getFilteredCountAdmin(DatatablesCriterias criterias) {
		StringBuilder queryBuilder = new StringBuilder("SELECT d FROM Soil d");
		queryBuilder.append(getFilterQueryForAdmin(criterias));
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery(queryBuilder.toString());
		List<Soil> list = (List<Soil>) q.list();
		tx.commit();
		session.close();
		return Long.parseLong(String.valueOf(list.size()));
	}
	@Override
	public List<Soil> getLatLonById(int id) {
		
		Session session= HibernateUtil.getSessionFactory().openSession();
		session.setFlushMode(FlushMode.ALWAYS);
		Transaction tx = session.beginTransaction();
		Query q = session.createQuery(" FROM Soil where gid='"+id+"'");		
		@SuppressWarnings("unchecked")
		List<Soil> list = (List<Soil>) q.list();

		
		tx.commit();
	    session.close();
	    return list;
	}
	

@SuppressWarnings("unchecked")
@Override
public List getSoilqueryzoomBoundary_all(String cqlquery,String tbname) {

	 ArrayList <Double> nv_codeList = new ArrayList<Double>();
	Connection conn=null;
	Double minxx = null;
	Double minyy = null;
	Double maxxx = null;
	Double maxyy = null;
	
	Double sminxx = null;
	Double sminyy = null;
	Double smaxxx = null;
	Double smaxyy = null;
	  ArrayList <Double> min_x = new ArrayList<Double>();
		ArrayList <Double> min_y = new ArrayList<Double>();
		ArrayList <Double> max_x = new ArrayList<Double>();
		ArrayList <Double> max_y = new ArrayList<Double>();
	 try{
		   conn=ConnectionProvider.getConnection();
		     Statement stmt = conn.createStatement();
	     String sql;
	     String name = null;
	      sql = "SELECT minx,miny,maxx,maxy FROM "+tbname+" where "+cqlquery;
	  
	      ResultSet rs = stmt.executeQuery(sql);
	    
	      while(rs.next()){
	    	  min_x.add(rs.getDouble("minx"));
	    	 min_y.add(rs.getDouble("miny"));
	    	   max_x.add(rs.getDouble("maxx"));
	    	   max_y.add(rs.getDouble("maxy"));
	    	  
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				
			
	      }
	      nv_codeList.add(minxx);
			 nv_codeList.add(minyy);
			 nv_codeList.add(maxxx);
			 nv_codeList.add(maxyy);
	     
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	
		return nv_codeList;
	}
@Override
public List<String> getEfficientZone() {
	
	List<String> columnNames = new ArrayList<String>();
	Connection conn = null;
	   try{
	  
		conn = ConnectionProvider.getConnection();
	    Statement stmt = conn.createStatement();
	    String sql;
	     
	   
	      sql="SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'efficiency_potential_crops_sis' and column_name LIKE '%_eff%';";
	    
	      ResultSet rs = stmt.executeQuery(sql);
	    
	    
	     
	     //STEP 5: Extract data from result set
  while(rs.next()){
        
	 
	
	  columnNames.add(rs.getString("column_name"));
	 
}
	     
	      //STEP 6: Clean-up environment
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	return columnNames;
}



@Override
public List<String> getPotentialZone() {
	
	List<String> columnNames = new ArrayList<String>();
	Connection conn = null;
	   try{
	  
		conn = ConnectionProvider.getConnection();
	    Statement stmt = conn.createStatement();
	    String sql;
	     
	   
	      sql="SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'efficiency_potential_crops_sis' and column_name LIKE '%_po%';";
	  
	      ResultSet rs = stmt.executeQuery(sql);
	    
	    
	     
	     //STEP 5: Extract data from result set
  while(rs.next()){
        
	  
	  columnNames.add(rs.getString("column_name"));
	
}
	      System.out.println("columns for potential zone."+columnNames);
	      //STEP 6: Clean-up environment
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	return columnNames;
}

@Override
public List<String> getEfficientZoneType(String effic_zone_type) {
	
	List<String> columnNames = new ArrayList<String>();
	Connection conn = null;
	   try{
	  
		conn = ConnectionProvider.getConnection();
	    Statement stmt = conn.createStatement();
	    String sql;
	     
	  
	      sql="select distinct("+effic_zone_type+") from efficiency_potential_crops_sis where "+effic_zone_type+" is not null order by "+effic_zone_type+"";
	  
	      ResultSet rs = stmt.executeQuery(sql);
	    
	    
	     
	     //STEP 5: Extract data from result set
  while(rs.next()){
        
	 
	  columnNames.add(rs.getString(effic_zone_type));
	
}
	  
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	return columnNames;
}


@Override
public List<String> getPotentialZoneType(String potential_zone_type) {
	
	List<String> columnNames = new ArrayList<String>();
	Connection conn = null;
	   try{
	  
		conn = ConnectionProvider.getConnection();
	    Statement stmt = conn.createStatement();
	    String sql;
	     
	   
	      sql="select distinct("+potential_zone_type+") from efficiency_potential_crops_sis where "+potential_zone_type+" is not null order by "+potential_zone_type+"";
	 
	      ResultSet rs = stmt.executeQuery(sql);
	    
	    
	     
	     //STEP 5: Extract data from result set
  while(rs.next()){
        
	 
	  columnNames.add(rs.getString(potential_zone_type));
	 
}
	     
	      //STEP 6: Clean-up environment
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	return columnNames;
}

//efficient zoom crop
@SuppressWarnings("unchecked")
@Override
public List getCropEffic_zoomBnd(String effic_crop,String effic_type) {

	 ArrayList <Double> nv_codeList = new ArrayList<Double>();
	Connection conn=null;
	Double minxx = null;
	Double minyy = null;
	Double maxxx = null;
	Double maxyy = null;
	
	Double sminxx = null;
	Double sminyy = null;
	Double smaxxx = null;
	Double smaxyy = null;
	  ArrayList <Double> min_x = new ArrayList<Double>();
		ArrayList <Double> min_y = new ArrayList<Double>();
		ArrayList <Double> max_x = new ArrayList<Double>();
		ArrayList <Double> max_y = new ArrayList<Double>();
	 try{
		   conn=ConnectionProvider.getConnection();
		     Statement stmt = conn.createStatement();
	     String sql;
	     String name = null;
	      sql = "SELECT minx,miny,maxx,maxy FROM efficiency_potential_crops_sis where "+effic_crop+"='"+effic_type+"'";
	      
	      ResultSet rs = stmt.executeQuery(sql);
	    
	      while(rs.next()){
	    	  min_x.add(rs.getDouble("minx"));
	    	 min_y.add(rs.getDouble("miny"));
	    	   max_x.add(rs.getDouble("maxx"));
	    	   max_y.add(rs.getDouble("maxy"));
	    	  
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				
			
	      }
	      nv_codeList.add(minxx);
			 nv_codeList.add(minyy);
			 nv_codeList.add(maxxx);
			 nv_codeList.add(maxyy);
	     
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	
		return nv_codeList;
	}

//potential zoom crop
@SuppressWarnings("unchecked")
@Override
public List getCropPoten_zoomBnd(String poten_crop,String poten_type) {

	 ArrayList <Double> nv_codeList = new ArrayList<Double>();
	Connection conn=null;
	Double minxx = null;
	Double minyy = null;
	Double maxxx = null;
	Double maxyy = null;
	
	Double sminxx = null;
	Double sminyy = null;
	Double smaxxx = null;
	Double smaxyy = null;
	  ArrayList <Double> min_x = new ArrayList<Double>();
		ArrayList <Double> min_y = new ArrayList<Double>();
		ArrayList <Double> max_x = new ArrayList<Double>();
		ArrayList <Double> max_y = new ArrayList<Double>();
	 try{
		   conn=ConnectionProvider.getConnection();
		     Statement stmt = conn.createStatement();
	     String sql;
	     String name = null;
	      sql = "SELECT minx,miny,maxx,maxy FROM efficiency_potential_crops_sis where "+poten_crop+"='"+poten_type+"'";
	
	      ResultSet rs = stmt.executeQuery(sql);
	    
	      while(rs.next()){
	    	  min_x.add(rs.getDouble("minx"));
	    	 min_y.add(rs.getDouble("miny"));
	    	   max_x.add(rs.getDouble("maxx"));
	    	   max_y.add(rs.getDouble("maxy"));
	    	  
				
				 minxx=Collections.min(min_x);
				 minyy=Collections.min(min_y);
				 maxxx=Collections.max(max_x);
				 maxyy=Collections.max(max_y);
				
			
	      }
	      nv_codeList.add(minxx);
			 nv_codeList.add(minyy);
			 nv_codeList.add(maxxx);
			 nv_codeList.add(maxyy);
	     
	      rs.close();
	      stmt.close();
	      conn.close();
	   }catch (SQLException e) {
			throw new RuntimeException(e);

		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}	
		return nv_codeList;
	}
}