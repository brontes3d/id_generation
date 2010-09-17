module IdGeneration
  include MyParamix

  mattr_accessor :generators
  
  def self.generator_for(klass)
    IdGeneration.generators[klass]
  end
  
  def self.included(base)
    # puts "IdGeneration included in #{base} " + base.mixin_parameters.inspect
    
    IdGeneration.generators ||= {}
    
    params = base.mixin_parameters[IdGeneration]
    id_param = params[:id]
    raise "no :id specified for IdGeneration on #{base}" unless id_param
    
    generator = params[:generator] || IdGenerator.sequential_generator_for("#{base.name}.#{id_param.to_s}")
    IdGeneration.generators[base] = generator
    
    base.validate do |record|
      if record.send(id_param.to_sym).blank?
        if record.new_record?
          record.generate_new_id
        else
          record.errors.add_on_blank(id_param.to_sym, "Can't be blank")
        end
      end
    end
    
    base.class_eval do
      define_method(:generate_new_id) do
        self.send("#{id_param.to_s}=".to_sym, generator.next_id)
      end
    end
    
    base.validates_uniqueness_of(id_param.to_sym)    
  end
  
end