package com.dao;

import java.util.List;
import java.util.Map;

import com.github.dandelion.datatables.core.ajax.DataSet;
import com.github.dandelion.datatables.core.ajax.DatatablesCriterias;
import com.models.Location;
import com.models.Soil;
import com.models.SoilAttribute;
import com.models.soil_health;

public interface SoilDAO {

	public List<Soil> getSoilReport();

	public Soil getSoilqueryzoomBoundary(String cqlquery);
	public DataSet<Soil> findSoilDetailWithDatatablesCriterias(DatatablesCriterias criterias, int userID);
	public List<Soil> getsur_form();

	public Soil getsur_formZoom(String sur_form);

	public List<Soil> getsoil_depth();
	public List<soil_health> getsoildistrict();
	public List<soil_health> getsoil_n_rating();
	public List<soil_health> getsoil_p_rating();
	public List<soil_health> getsoil_ec_rating();
	public List<soil_health> getsoil_ph_rating();
	public List<soil_health> getsoil_k_rating();
	public List<soil_health> getsoil_oc_rating();
	public List<soil_health> getsoil_zn_rating();
	public List<soil_health> getsoil_fe_rating();
	public List<soil_health> getsoil_cu_rating();
	public List<soil_health> getsoil_mn_rating();
	public List<soil_health> getsoil_b_rating();
	public List<soil_health> getsoil_s_rating();
	
	public Soil getsoil_depthZoom(String soil_depth);

	public List<Soil> getsoil_react();

	public Soil getsoil_reactZoom(String soil_react);

	public List<Soil> getsoil_drain();

	public Soil getsoil_drainZoom(String soil_drain);

	public Soil getsoil_textZoom(String soil_text);

	public List<Soil> getsoil_text();

	public List<Soil> geterosion();

	public Soil geterosionZoom(String erosion);

	public List<Soil> getslope();

	public Soil getslopeZoom(String slope);

	public List<SoilAttribute> getsalinity();

	public List<SoilAttribute> getsodicity();

	public List<SoilAttribute> getacidity();
	public List<Soil> getLatLonById(int id);

	public List<Soil> getSoilQueryReport(String cqlquery, String tbname);

	public List<Map<String, Object>> getSoilQueryReport_all(String cqlquery, String tbname);

	public List getSoilqueryzoomBoundary_all(String cqlquery, String tbname);

	public List<String> getEfficientZone();

	public List<String> getPotentialZone();

	public List<String> getEfficientZoneType(String effic_zone_type);

	public List<String> getPotentialZoneType(String potential_zone_type);

	public List getCropEffic_zoomBnd(String effic_crop, String effic_type);

	public List getCropPoten_zoomBnd(String poten_crop, String poten_type);

	
}
